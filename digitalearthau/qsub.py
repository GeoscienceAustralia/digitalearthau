import getpass
import json
import multiprocessing
import signal
import socket
import uuid
from typing import Optional, List, Iterable

import celery
import click
import datetime
import yaml
import re
import logging
import itertools
import collections
import shlex
from time import sleep

from celery.events import EventReceiver
from dateutil import tz
from pydash import pick
from pathlib import Path
from functools import update_wrapper
from subprocess import Popen, PIPE

from digitalearthau.events import Status, TaskEvent, NodeMessage
from . import pbs
from datacube.executor import (SerialExecutor,
                               mk_celery_executor,
                               _get_concurrent_executor,
                               _get_distributed_executor)

from datacube import _celery_runner as cr

import celery.events.state as celery_state
import sys

import celery.states

from . import events

NUM_CPUS_PER_NODE = 16
QSUB_L_FLAGS = 'mem ncpus walltime wd'.split(' ')

PASS_THRU_KEYS = 'name project queue env_vars wd noask _internal'.split(' ')
VALID_KEYS = PASS_THRU_KEYS + 'walltime ncpus nodes mem extra_qsub_args'.split(' ')

_LOG = logging.getLogger(__name__)


class QSubLauncher(object):
    """ This class is for self-submitting as a PBS job.
    """

    def __init__(self, params, internal_args=None):
        """
        params -- normalised dictionary of qsub options, see `norm_qsub_params`
        internal_args -- optional extra command line arguments to add
                         when launching, particularly useful when using auto-mode
                         that passes through sys.argv arguments
        """
        self._internal_args = internal_args
        self._params = params

    def __repr__(self):
        return yaml.dump(dict(qsub=self._params))

    def add_internal_args(self, *args):
        if self._internal_args is None:
            self._internal_args = args
        else:
            self._internal_args = self._internal_args + args

    def __call__(self, *args, **kwargs):
        """ Submit self via qsub

        auto=True -- re-use arguments used during invocation, removing `--qsub` parameter
        auto=False -- ignore invocation arguments and just use suplied *args

        args -- command line arguments to launch with under qsub, only used if auto=False
        """
        auto = kwargs.get('auto', False)
        if auto:
            args = sys.argv[1:]
            args = remove_args('--qsub', args, n=1)
            args = remove_args('--queue-size', args, n=1)
            args = tuple(args)

        if self._internal_args is not None:
            args = tuple(self._internal_args) + args

        r, output = qsub_self_launch(self._params, *args)
        click.echo(output)
        return r, output


class QSubParamType(click.ParamType):
    name = 'opts'

    def convert(self, value, param, ctx):

        if value == 'help':
            click.echo('''
Following parameters are understood:

   nodes    = <int> number of nodes
   ncpus    = <int> number of cores if your don't need whole node
   walltime = <duration> e.g. "10m" -- ten minutes, "5h" -- five hours
   mem      = (small|medium|high) or 2G, 4G memory per core
   name     = job name
   project  = (u46|v10 etc.)
   queue    = (normal|express)
   noask do not ask for confirmation

Put one pameter per line or use commas to separate parameters.

Examples:
   --qsub 'nodes=10,walltime=3h,project=v10'
   --qsub 'name = my-task
   nodes = 7
   mem = medium
   walltime = 30m
   noask'
''')
            ctx.exit()

        try:
            p = parse_comma_args(value, VALID_KEYS)

            if 'wd' not in p:
                p['wd'] = True

            p = norm_qsub_params(p)
            return QSubLauncher(p, ('--celery', 'pbs-launch'))
        except ValueError:
            self.fail('Failed to parse: {}'.format(value), param, ctx)


def remove_args(opt, args, n=1):
    out = []
    skip = 0

    for a in args:
        if skip > 0:
            skip -= 1
            continue

        if a == opt:
            skip = n
        elif a.startswith(opt + '='):
            skip = 0
        else:
            out.append(a)

    return out


class HostPort(click.ParamType):
    name = 'host:port'

    def __init__(self, default_port=None):
        self._default_port = default_port

    def convert(self, value, param, ctx):
        if value is None:
            return None
        hp = value.split(':')
        if len(hp) == 1:
            return (value, self._default_port)

        if len(hp) > 2:
            self.fail('Expect value in <host:port> format')

        host, port = hp
        try:
            port = int(port)
        except ValueError:
            self.fail('Expect value in <host:port> format, where port is an integer')

        return (host, port)


def parse_comma_args(s, valid_keys=None):
    def parse_one(a):
        kv = tuple(s.strip() for s in re.split(' *[=:] *', a))
        if len(kv) == 1:
            kv = (kv[0], True)

        if len(kv) != 2:
            raise ValueError('Bad option: ' + a)

        if valid_keys:
            k = kv[0]
            if k not in valid_keys:
                raise ValueError('Unexpected key:' + k)

        return kv

    return dict(parse_one(a) for a in re.split('[,;\n]', s) if a != '')


def normalise_walltime(x):
    if x is None or x.find(':') >= 0:
        return x

    m = re.match('^([0-9]+) *([hms]|min|minutes|hours)?$', x)
    if m is None:
        return None

    aliases = {'hours': 'h',
               None: 'h',
               'min': 'm',
               'minutes': 'm'}

    scale = dict(h=60 * 60,
                 m=60,
                 s=1)

    def fmt(secs):
        h = secs // (60 * 60)
        m = (secs // 60) % 60
        s = secs % 60
        return '{}:{:02}:{:02}'.format(h, m, s)

    v, units = m.groups()
    units = aliases.get(units, units)
    return fmt(int(v) * scale[units])


def normalise_mem(x):
    named = dict(small=2,
                 medium=4,
                 large=7.875)

    if x in named:
        return named[x]

    m = re.match('^ *([0-9]+) *([g|G][bB]*)* *$', x)
    if m is None:
        return None
    return int(m.groups()[0])


def norm_qsub_params(p):
    ncpus = int(p.get('ncpus', 0))

    if ncpus == 0:
        nodes = int(p.get('nodes', 1))
        ncpus = nodes * NUM_CPUS_PER_NODE
    else:
        nodes = None

    mem = normalise_mem(p.get('mem', 'small'))

    if nodes:
        mem = int((mem * NUM_CPUS_PER_NODE * 1024 - 512) * nodes)
    else:
        mem = int(mem * ncpus * 1024)

    mem = '{}MB'.format(mem)

    walltime = normalise_walltime(p.get('walltime'))

    extra_qsub_args = p.get('extra_qsub_args', [])
    if isinstance(extra_qsub_args, str):
        extra_qsub_args = extra_qsub_args.split(' ')

    pp = dict(ncpus=ncpus,
              mem=mem,
              walltime=walltime,
              extra_qsub_args=extra_qsub_args)

    pp.update(pick(p, PASS_THRU_KEYS))

    return pp


def build_qsub_args(**p):
    args = []

    flags = dict(project='-P',
                 queue='-q',
                 name='-N')

    def add_l_arg(n):
        v = p.get(n)
        if v is not None:
            if isinstance(v, bool):
                if v:
                    args.append('-l{}'.format(n))
            else:
                args.append('-l{}={}'.format(n, v))

    def add_arg(n):
        v = p.get(n)
        if v is not None:
            flag = flags[n]
            args.extend([flag, v])

    for n in QSUB_L_FLAGS:
        add_l_arg(n)

    for n in flags:
        add_arg(n)

    args.extend(p.get('extra_qsub_args', []))

    # TODO: deal with env_vars!

    return args


def self_launch_args(*args):
    """
    Build tuple in the form (current_python, current_script, *args)
    """

    py_file = str(Path(sys.argv[0]).absolute())
    return (sys.executable, py_file) + args


def generate_self_launch_script(*args):
    s = "#!/bin/bash\n\n"
    s += pbs.generate_env_header()
    s += '\n\nexec ' + ' '.join(shlex.quote(s) for s in self_launch_args(*args))
    return s


def qsub_self_launch(qsub_opts, *args):
    script = generate_self_launch_script(*args)
    qsub_args = build_qsub_args(**qsub_opts)

    noask = qsub_opts.get('noask', False)

    if not noask:
        click.echo('Args: ' + ' '.join(map(str, args)))
        confirmed = click.confirm('Submit to pbs?')
        if not confirmed:
            return (0, 'Aborted by user')

    proc = Popen(['qsub'] + qsub_args, stdin=PIPE, stdout=PIPE)
    proc.stdin.write(script.encode('utf-8'))
    proc.stdin.close()
    out_txt = proc.stdout.read().decode('utf-8')
    exit_code = proc.wait()

    return exit_code, out_txt


# The strigified args that celery gives us back within task messages
_EXAMPLE_TASK_ARGS = "'(functools.partial(<function do_fc_task at 0x7f47e7aad598>, {" \
                     "\'source_type\': \'ls8_nbar_albers\', \'output_type\': \'ls8_fc_albers\', " \
                     "\'version\': \'${version}\', \'description\': \'Landsat 8 Fractional Cover 25 metre, " \
                     "100km tile, Australian Albers Equal Area projection (EPSG:3577)\', \'product_type\': " \
                     "\'fractional_cover\', \'location\': \'/g/data/fk4/datacube/002/\', \'file_path_template\': " \
                     "\'LS8_OLI_FC/{tile_index[0]}_{tile_index[1]}/LS8_OLI_FC_3577_{tile_index[0]}_{tile_index[1]}_" \
                     "{start_time}_v{version}.nc\', \'partial_ncml_path_template\': \'LS8_OLI_FC/{tile_index[0]}_" \
                     "{tile_index[1]}/LS8_OLI_FC_3577_{tile_index[0]}_{tile_index[1]}_{start_time}.ncml\', \'ncml_" \
                     "path_template\': \'LS8_OLI_FC/LS8_OLI_FC_3577_{tile_index[0]}_{tile_index[1]}.ncml\', \'sensor" \
                     "_regression_coefficients\': {\'blue\': [0.00041, 0.9747], \'green\': [0.00289, 0.99779], " \
                     "\'red\': [0.00274, 1.00446], \'nir\': [4e-05, 0.98906], \'swir1\': [0.00256, 0.99467], " \
                     "\'swir2\': [-0.00327, 1.02551]}, \'global_attributes\': {\'title\': \'Fractional Cover 25 " \
                     "v2\', \'summary\': \"The Fractional Cover (FC)...,)'"
_EXAMPLE_TASK_KWARGS = "{'task': {'nbar': Tile<sources=<xarray.DataArray (time: 1)>\narray([ (Dataset <id=" \
                       "d514c26a-d98f-47f1-b0de-15f7fe78c209 type=ls8_nbar_albers location=/g/data/rs0/datacube/002/" \
                       "LS8_OLI_NBAR/-11_-28/LS8_OLI_NBAR_3577_-11_-28_2015_v1496400956.nc>,)], dtype=object)\n" \
                       "Coordinates:\n  * time     (time) datetime64[ns] 2015-01-31T01:51:03,\n\tgeobox=GeoBox(4000, " \
                       "4000, Affine(25.0, 0.0, -1100000.0,\n       0.0, -25.0, -2700000.0), EPSG:3577)>, " \
                       "'tile_index': (-11, -28, numpy.datetime64('2015-01-31T01:51:03.000000000')), " \
                       "'filename': '/g/data/fk4/datacube/002/LS8_OLI_FC/-11_-28/LS8_OLI_FC_3577_-11_-28_" \
                       "20150131015103000000_v1507076205.nc'}}"

TASK_ID_RE_EXTRACT = re.compile('Dataset <id=([a-z0-9-]{36}) ')


def _extract_task_args_dataset_id(kwargs: str) -> Optional[uuid.UUID]:
    """
    >>> _extract_task_args_dataset_id(_EXAMPLE_TASK_KWARGS)
    UUID('d514c26a-d98f-47f1-b0de-15f7fe78c209')
    >>> _extract_task_args_dataset_id("no match")
    """
    m = TASK_ID_RE_EXTRACT.search(kwargs)
    if not m:
        return None

    return uuid.UUID(m.group(1))


def get_task_input_dataset_id(task: celery_state.Task):
    return _extract_task_args_dataset_id(task.kwargs)


def celery_event_to_task(name, task: celery_state.Task, user=getpass.getuser()) -> Optional[TaskEvent]:
    # root_id uuid ?
    # uuid    uuid
    # hostname, pid
    # retries ?
    # timestamp ?
    # state ("RECEIVED")

    celery_statemap = {
        celery.states.PENDING: Status.PENDING,
        # Task was received by a worker.
        celery.states.RECEIVED: Status.PENDING,
        celery.states.STARTED: Status.ACTIVE,
        celery.states.SUCCESS: Status.COMPLETE,
        celery.states.FAILURE: Status.FAILED,
        celery.states.REVOKED: Status.CANCELLED,
        # Task was rejected (by a worker?)
        celery.states.REJECTED: Status.CANCELLED,
        # Waiting for retry
        celery.states.RETRY: Status.PENDING,
        celery.states.IGNORED: Status.PENDING,
    }
    if not task.state:
        _LOG.warning("No state known for task %r", task)
        return None
    status = celery_statemap.get(task.state)
    if not status:
        raise RuntimeError("Unknown celery state %r" % task.state)

    message = None
    if status.FAILED:
        message = task.traceback

    celery_worker: celery_state.Worker = task.worker
    dataset_id = get_task_input_dataset_id(task)
    return TaskEvent(
        timestamp=_utc_datetime(task.timestamp) if task.timestamp else datetime.datetime.utcnow(),
        event=f"task.{status.name.lower()}",
        name=name,
        user=user,
        status=status,
        id=task.id,
        parent_id=pbs.current_job_task_id(),
        message=message,
        input_datasets=(dataset_id,) if dataset_id else None,
        output_datasets=None,
        node=NodeMessage(
            hostname=_just_hostname(celery_worker.hostname),
            pid=celery_worker.pid
        ),
    )


def _utc_datetime(timestamp: float):
    """
    >>> _utc_datetime(1507241505.7179525)
    datetime.datetime(2017, 10, 5, 22, 11, 45, 717952, tzinfo=tzutc())
    """
    return datetime.datetime.utcfromtimestamp(timestamp).replace(tzinfo=tz.tzutc())


def _just_hostname(hostname: str):
    """
    >>> _just_hostname("kveikur.local")
    'kveikur.local'
    >>> _just_hostname("someone@kveikur.local")
    'kveikur.local'
    >>> _just_hostname("someone@kveikur@local")
    Traceback (most recent call last):
    ...
    ValueError: ...
    """
    if '@' not in hostname:
        return hostname

    parts = hostname.split('@')
    if len(parts) != 2:
        raise ValueError("Strange-looking, unsupported hostname %r" % (hostname,))

    return parts[-1]


def log_celery_tasks(should_shutdown: multiprocessing.Value, app: celery.Celery):
    # Open log file.
    # Connect to celery
    # Stream events to file.

    click.secho("Starting logger", bold=True)
    state: celery_state.State = app.events.State()

    # TODO: handling immature shutdown cleanly? The celery runner itself might need better support for it...

    # For now we ignore these "gentle" shutdown signals as we don't want to quit until all logs have been received.
    # The main process will receive sigints/terms and will tell us ("should_shutdown" var) when it's safe...
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)

    def handle_task(event):

        state.event(event)

        event_type: str = event['type']

        if not event_type.startswith('task-'):
            _LOG.debug("Skipping event %r", event_type)
            return

        # task name is sent only with -received event, and state
        # will keep track of this for us.
        task: celery_state.Task = state.tasks.get(event['uuid'])

        if not task:
            _LOG.warning(f"No task found {event_type}")
            return
        output.write_item(celery_event_to_task('fc.run', task))
        _log_task_states(state)

    with events.JsonLinesWriter(Path('app-events.jsonl').open('a')) as output:
        with app.connection() as connection:

            recv: EventReceiver = app.events.Receiver(connection, handlers={
                '*': handle_task,
            })

            # If idle for 5 seconds, it will recheck whether to shutdown
            while not should_shutdown.value:
                try:
                    for _ in recv.consume(limit=None, timeout=5, wakeup=True):
                        pass
                except socket.timeout:
                    pass
            _LOG.info("logger finished")

    _log_task_states(state)

    # According to our recorded state we should have seen all workers stop.
    workers: List[celery_state.Worker] = list(state.workers.values())
    active_workers = [w.hostname for w in workers if w.active]
    _LOG.info("%s/%s recorded workers are active", len(active_workers), len(workers))
    # Based on recency of heartbeat, not an offline event.
    _LOG.info("%s/%s recorded workers seem to be alive", len(list(state.alive_workers())), len(workers))

    if active_workers:
        _LOG.warning(
            "Some workers had not finished executing; their logs will be missed:n\n\t%s",
            "\n\t".join(active_workers)
        )


def _log_task_states(state):
    # Print count of tasks in each state.
    tasks: Iterable[celery_state.Task] = state.tasks.values()
    task_states = collections.Counter(t.state for t in tasks)
    _LOG.info("Task states: %s", ", ".join(f"{v} {k}" for (k, v) in task_states.items()))


# TODO: Refactor before pull request (Hopefully this comment doesn't enter the pull request, that would be embarrassing)
# pylint: disable=too-many-locals
def launch_redis_worker_pool(port=6379, **redis_params):
    redis_port = port
    redis_host = pbs.hostname()
    redis_password = cr.get_redis_password(generate_if_missing=True)

    redis_shutdown = cr.launch_redis(redis_port, redis_password, **redis_params)
    if not redis_shutdown:
        raise RuntimeError('Failed to launch Redis')

    _LOG.info('Launched Redis at %s:%d', redis_host, redis_port)

    for i in range(5):
        if cr.check_redis(redis_host, redis_port, redis_password) is False:
            sleep(0.5)

    executor = cr.CeleryExecutor(
        redis_host,
        redis_port,
        password=redis_password,
    )
    logger_shutdown = multiprocessing.Value('b', False, lock=False)
    log_proc = multiprocessing.Process(target=log_celery_tasks, args=(logger_shutdown, cr.app,))
    log_proc.start()

    worker_env = pbs.get_env()
    worker_procs = []

    for node in pbs.nodes():
        nprocs = node.num_cores
        if node.is_main:
            nprocs = max(1, nprocs - 2)

        celery_worker_script = 'exec datacube-worker --executor celery {}:{} --nprocs {}'.format(
            redis_host, redis_port, nprocs)
        proc = pbs.pbsdsh(node.offset, celery_worker_script, env=worker_env)
        _LOG.info(f"Started {proc.pid}")
        worker_procs.append(proc)

    def start_shutdown():
        cr.app.control.shutdown()

    def shutdown():
        start_shutdown()
        _LOG.info('Waiting for workers to quit')

        # TODO: time limit followed by kill
        for p in worker_procs:
            p.wait()

        # We deliberately don't want to stop the logger until all worker have stopped completely.
        _LOG.info('Stopping log process')
        logger_shutdown.value = True
        log_proc.join()

        _LOG.info('Shutting down redis-server')
        redis_shutdown()

    return executor, shutdown


def describe_task(task):
    """ Convert task to string for logging
    """
    if hasattr(task, 'get'):
        t_idx = task.get('tile_index')
        if t_idx is not None:
            return str(t_idx)
    return repr(task)


def run_tasks(tasks, executor, run_task, process_result=None, queue_size=50):
    """

    :param tasks: iterable of tasks. Usually a generator to create them as required.
    :param executor: a datacube executor, similar to `distributed.Client` or `concurrent.futures`
    :param run_task: the function used to run a task. Expects a single argument of one of the tasks
    :param process_result: a function to do something based on the result of a completed task. It
                           takes a single argument, the return value from `run_task(task)`
    :param queue_size: How large the queue of tasks should be. Will depend on how fast tasks are
                       processed, and how much memory is available to buffer them.
    """
    _LOG.debug('Starting running tasks...')
    results = []
    task_queue = itertools.islice(tasks, queue_size)
    for task in task_queue:
        _LOG.info('Running task: %s', describe_task(task))
        results.append(executor.submit(run_task, task=task))

        _LOG.debug('Task queue filled, waiting for first result...')

    successful = failed = 0
    while results:
        result, results = executor.next_completed(results, None)

        # submit a new _task to replace the one we just finished
        task = next(tasks, None)
        if task:
            _LOG.info('Running _task: %s', describe_task(task))
            results.append(executor.submit(run_task, task=task))

        # Process the result
        try:
            actual_result = executor.result(result)
            if process_result is not None:
                process_result(actual_result)
            successful += 1
        except Exception as err:  # pylint: disable=broad-except
            _LOG.exception('Task failed: %s', err)
            failed += 1
            continue
        finally:
            # Release the _task to free memory so there is no leak in executor/scheduler/worker process
            executor.release(result)

    _LOG.info('%d successful, %d failed', successful, failed)
    return successful, failed


class TaskRunner(object):
    def __init__(self, kind='serial', opts=None):
        self._kind = kind
        self._opts = opts
        self._executor = None
        self._shutdown = None
        self._queue_size = None
        self._user_queue_size = None

    def __repr__(self):
        args = '' if self._opts is None else '-{}'.format(str(self._opts))
        return '{}{}'.format(self._kind, args)

    def set_qsize(self, qsize):
        self._user_queue_size = qsize

    def start(self):
        def noop():
            pass

        def mk_pbs_celery():
            qsize = pbs.preferred_queue_size()
            port = 6379  # TODO: randomise
            maxmemory = "1024mb"  # TODO: compute maxmemory from qsize
            executor, shutdown = launch_redis_worker_pool(port=port, maxmemory=maxmemory)
            return (executor, qsize, shutdown)

        def mk_dask():
            qsize = 100
            executor = _get_distributed_executor(self._opts)
            return (executor, qsize, noop)

        def mk_celery():
            qsize = 100
            executor = mk_celery_executor(*self._opts)
            return (executor, qsize, noop)

        def mk_multiproc():
            qsize = 100
            executor = _get_concurrent_executor(self._opts)
            return (executor, qsize, noop)

        def mk_serial():
            qsize = 10
            executor = SerialExecutor()
            return (executor, qsize, noop)

        mk = dict(pbs_celery=mk_pbs_celery,
                  celery=mk_celery,
                  dask=mk_dask,
                  multiproc=mk_multiproc,
                  serial=mk_serial)

        try:
            (self._executor,
             default_queue_size,
             self._shutdown) = mk.get(self._kind, mk_serial)()
        except RuntimeError:
            _LOG.exception("Error starting executor")
            return False

        if self._user_queue_size is not None:
            self._queue_size = self._user_queue_size
        else:
            self._queue_size = default_queue_size

    def stop(self):
        if self._shutdown is not None:
            self._shutdown()
            self._executor = None
            self._queue_size = None
            self._shutdown = None

    def __call__(self, tasks, run_task, on_task_complete=None):
        if self._executor is None:
            if self.start() is False:
                raise RuntimeError('Failed to launch worker pool')

        return run_tasks(tasks, self._executor, run_task, on_task_complete, self._queue_size)


def get_current_obj(ctx=None):
    if ctx is None:
        ctx = click.get_current_context()

    if ctx.obj is None:
        ctx.obj = {}
    return ctx.obj


class QsubRunState:
    def __init__(self) -> None:
        self.runner: TaskRunner = None
        self.qsub: QSubLauncher = None
        self.qsize: int = None


def with_qsub_runner():
    """
    Will add the following options

    --parallel <int>
    --dask 'host:port'
    --celery 'host:port'|'pbs-launch'
    --queue-size <int>
    --qsub <qsub-params>

    Will populate variables
      qsub   - None | QSubLauncher
      runner - None | TaskRunner
    """

    arg_name = 'runner'
    o_key = '_qsub_state'

    def state(ctx=None) -> QsubRunState:
        obj = get_current_obj(ctx)
        if o_key not in obj:
            obj[o_key] = QsubRunState()
        return obj[o_key]

    def add_multiproc_executor(ctx, param, value):
        if value is None:
            return
        state(ctx).runner = TaskRunner('multiproc', value)

    def add_dask_executor(ctx, param, value):
        if value is None:
            return
        state(ctx).runner = TaskRunner('dask', value)

    def add_celery_executor(ctx, param, value):
        if value is None:
            return
        if value[0] == 'pbs-launch':
            state(ctx).runner = TaskRunner('pbs_celery')
        else:
            state(ctx).runner = TaskRunner('celery', value)

    def add_qsize(ctx, param, value):
        if value is None:
            return
        state(ctx).qsize = value

    def capture_qsub(ctx, param, value):
        state(ctx).qsub = value
        return value

    def decorate(f):
        opts = [
            click.option('--parallel',
                         type=int,
                         help='Run locally in parallel',
                         expose_value=False,
                         callback=add_multiproc_executor),
            click.option('--dask',
                         type=HostPort(),
                         help=(
                             'Use dask.distributed backend for parallel computation. '
                             'Supply address of dask scheduler.'
                         ),
                         expose_value=False,
                         callback=add_dask_executor),
            click.option('--celery',
                         type=HostPort(),
                         help=(
                             'Use celery backend for parallel computation. '
                             'Supply redis server address, or "pbs-launch" to launch redis '
                             'server and workers when running under pbs.'
                         ),
                         expose_value=False,
                         callback=add_celery_executor),
            click.option('--queue-size',
                         type=int,
                         help='Overwrite defaults for queue size',
                         expose_value=False,
                         callback=add_qsize),
            click.option('--qsub',
                         type=QSubParamType(),
                         callback=capture_qsub,
                         help=(
                             'Launch via qsub, supply comma or new-line separated list of parameters.'
                             ' Try --qsub=help.'
                         )),
        ]

        for o in opts:
            f = o(f)

        def finalise_state():
            s = state()
            if s.runner is None and s.qsub is None:
                s.runner = TaskRunner()

            if s.runner is not None and s.qsize is not None:
                s.runner.set_qsize(s.qsize)

            if s.qsub is not None and s.qsize is not None:
                s.qsub.add_internal_args('--queue-size', s.qsize)

        def extract_runner(*args, **kwargs):
            finalise_state()
            kwargs.update({arg_name: state().runner})
            return f(*args, **kwargs)

        return update_wrapper(extract_runner, f)

    return decorate
