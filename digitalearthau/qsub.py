import itertools
import logging
import re
import shlex
import sys
from functools import update_wrapper
from pathlib import Path
from subprocess import Popen, PIPE
from pprint import pprint
import click
import yaml
from pydash import pick
from typing import List, Tuple

from datacube.executor import (SerialExecutor,
                               mk_celery_executor,
                               _get_concurrent_executor,
                               _get_distributed_executor)
from . import pbs
from .runners import celery_environment
from .runners.model import TaskDescription

NUM_CPUS_PER_NODE = 16
QSUB_L_FLAGS = 'mem ncpus walltime wd'.split(' ')

# Keys that can be sent as-is to the qsub builder without normalisation (I think?)
PASS_THRU_KEYS = 'name project queue env_vars wd noask umask stdout stderr'.split(' ')
# All keys/arguments supported by QsubLauncher functions
VALID_KEYS = PASS_THRU_KEYS + 'walltime ncpus nodes mem extra_qsub_args'.split(' ')

_LOG = logging.getLogger(__name__)


class QSubLauncher(object):
    """ This class is for self-submitting as a PBS job.
    """

    def __init__(self, params, internal_args=None, auto_clean=None):
        """
        params -- normalised dictionary of qsub options, see `norm_qsub_params`
        internal_args -- optional extra command line arguments to add
                         when launching, particularly useful when using auto-mode
                         that passes through sys.argv arguments
        auto_clean -- params to remove when self-submitting
        """
        self._raw_qsub_params = params
        self._internal_args = internal_args
        self._auto_clean = auto_clean

    def __repr__(self):
        return yaml.dump(dict(qsub=self._raw_qsub_params))

    def clone(self):
        return QSubLauncher(self._raw_qsub_params, self._internal_args, self._auto_clean)

    def add_internal_args(self, *args):
        if self._internal_args is None:
            self._internal_args = args
        else:
            self._internal_args = self._internal_args + args

    def reset_internal_args(self):
        self._internal_args = None

    def __call__(self, *args, auto=False, auto_clean=None, **kwargs):
        """ Submit self via qsub

        auto=True -- re-use arguments used during invocation, removing `--qsub` parameter
        auto=False -- ignore invocation arguments and just use suplied *args

        args -- command line arguments to launch with under qsub, only used if auto=False

        auto_clean -- extra list of params to remove
        """
        if auto:
            args = sys.argv[1:]

            def to_pairs(ls):
                if ls is None:
                    return []

                return [(x, 0) if isinstance(x, str) else x
                        for x in ls]

            for arg, nargs in to_pairs(self._auto_clean) + to_pairs(auto_clean):
                args = remove_args(arg, args, n=nargs)
            args = tuple(args)

        qsub_args, script = self.build_submission(*args)

        if not self._raw_qsub_params.get('noask', False):
            click.echo('Args: ' + ' '.join(map(str, args)))
            confirmed = click.confirm('Submit to pbs?')
            if not confirmed:
                return 0, 'Aborted by user'

        proc = Popen(['qsub'] + qsub_args, stdin=PIPE, stdout=PIPE)
        proc.stdin.write(script.encode('utf-8'))
        proc.stdin.close()
        out_txt = proc.stdout.read().decode('utf-8')
        exit_code = proc.wait()
        return exit_code, out_txt

    # A slighly higher-level alternative to the above, since we repeatedly want to get the job_id and
    # die on errors. This one captures rather than prints stderr.
    # PyCharm also doesn't like type definitions on __call__()s.
    # (TODO Leaving the above api for now as other scripts may break and we're on a deadline...)
    def submit(self, *commands: str) -> str:
        """
        Submit a job, returning the job_id.

        Throws JobSubmissionError on failure.
        """
        qsub_args, script = self.build_submission(*commands)

        proc = Popen(['qsub'] + qsub_args, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        proc.stdin.write(script.encode('utf-8'))
        proc.stdin.close()
        stdout = proc.stdout.read().decode('utf-8')
        stderr = proc.stdout.read().decode('utf-8')
        exit_code = proc.wait()

        if exit_code != 0:
            raise JobSubmissionError("Error submitting qsub job", stdout, stderr)

        job_id = stdout.strip(' \n')
        return job_id

    def build_submission(self, *commands: str) -> Tuple[List[str], str]:
        """Get the qsub arguments and script that would be submitted, but don't submit it."""
        if self._internal_args is not None:
            commands = tuple(self._internal_args) + commands

        script = _generate_self_launch_script(*commands)
        qsub_args = _build_qsub_args(**self._raw_qsub_params)
        return qsub_args, script


class JobSubmissionError(Exception):
    pass


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
            return QSubLauncher(p, ('--celery', 'pbs-launch'),
                                [('--qsub', 1), ('--queue-size', 1)])
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
    """
    >>> normalise_walltime('4h')
    '4:00:00'
    >>> # TODO Nothing returned or errored?
    >>> normalise_walltime('4h5m')
    """
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
    """
    >>> normalise_mem('2gb')
    2
    >>> normalise_mem('medium')
    4
    """
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
    """
    >>> pprint(norm_qsub_params({
    ...    'mem': 'small',
    ...    'wd': True,
    ...    'noask': True,
    ...    'nodes': 4,
    ...    'walltime': '4h',
    ...    'project': 'v10',
    ...    'queue': 'normal',
    ...    'stdout': 'test.out.txt',
    ...    'stderr': 'test.err.txt',
    ...    'umask': 33,
    ...    'name': 'staggering-fc-run',
    ... }))
    {'extra_qsub_args': [],
     'mem': '129024MB',
     'name': 'staggering-fc-run',
     'ncpus': 64,
     'noask': True,
     'project': 'v10',
     'queue': 'normal',
     'stderr': 'test.err.txt',
     'stdout': 'test.out.txt',
     'umask': 33,
     'walltime': '4:00:00',
     'wd': True}
    >>> # Default params
    >>> norm_qsub_params({})
    {'ncpus': 16, 'mem': '32256MB', 'walltime': None, 'extra_qsub_args': []}
    >>> # TODO error on unknown args? It seems to explicitly pass through (PASS_THRU_KEYS) some, but not all valid keys?
    >>> # No error currently:
    >>> # norm_qsub_params({'ubermensch': 'understood'})
    """
    ncpus = int(p.pop('ncpus', 0))

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


def _build_qsub_args(**p):
    """
    Turn the output of norm_qsub_params into arguments for qsub.

    >>> _build_qsub_args(
    ...     ncpus=1,
    ...     mem='4096MB',
    ...     walltime='1:00:00',
    ...     name='test',
    ...     project='u46',
    ...     queue='normal',
    ...     wd=True,
    ...     noask=True
    ... )
    ['-lmem=4096MB', '-lncpus=1', '-lwalltime=1:00:00', '-lwd', '-P', 'u46', '-q', 'normal', '-N', 'test']
    >>> # It should complain on unknown fields
    >>> _build_qsub_args(project='v10', wrong_arg='lumberjack')
    Traceback (most recent call last):
    ...
    ValueError: Unknown qsub arguments: {'wrong_arg': 'lumberjack'}
    >>> _build_qsub_args(stdout='/tmp/testout.txt', stderr=Path('/tmp/testerr.txt'), umask=33)
    ['-o', '/tmp/testout.txt', '-e', '/tmp/testerr.txt', '-W', 'umask=33']
    """

    args = []

    flags = dict(project='-P',
                 queue='-q',
                 name='-N',
                 stdout='-o',
                 stderr='-e')

    def add_l_arg(n):
        v = p.pop(n, None)
        if v is not None:
            if isinstance(v, bool):
                if v:
                    args.append('-l{}'.format(n))
            else:
                args.append('-l{}={}'.format(n, v))

    def add_arg(n):
        v = p.pop(n, None)
        if v is not None:
            flag = flags[n]
            args.extend([flag, str(v)])

    for n in QSUB_L_FLAGS:
        add_l_arg(n)

    for n in flags:
        add_arg(n)

    # Umask is in a an extended attributes group ("-W")
    umask = p.pop('umask', None)
    if umask:
        args.extend(['-W', f'umask={umask}'])

    args.extend(p.pop('extra_qsub_args', []))

    # Used in UI that calls this method, not qsub.
    p.pop('noask', None)

    # TODO: deal with env_vars!
    # For now, throw error if someone tries to set one (empty is ok)
    if p.pop('env_vars', None):
        raise NotImplementedError("env_vars is not yet implemented")

    # We should have popped all input arguments, otherwise they passed something unknown
    if p:
        raise ValueError(f"Unknown qsub arguments: {repr(p)}")

    return args


def self_launch_args(*args):
    """
    Build tuple in the form (current_python, current_script, *args)
    """

    py_file = str(Path(sys.argv[0]).absolute())
    return (sys.executable, py_file) + args


def _generate_self_launch_script(*args):
    s = "#!/bin/bash\n\n"
    s += pbs.generate_env_header()
    s += '\n\nexec ' + ' '.join(shlex.quote(s) for s in self_launch_args(*args))
    return s


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
        self._workers_per_node = None

    def __repr__(self):
        args = '' if self._opts is None else '-{}'.format(str(self._opts))
        return '{}{}'.format(self._kind, args)

    def set_qsize(self, qsize):
        self._user_queue_size = qsize

    def set_workers_per_node(self, value):
        self._workers_per_node = value

    def start(self, task_desc: TaskDescription = None):
        def noop():
            pass

        def mk_pbs_celery(task_desc: TaskDescription):
            qsize = pbs.preferred_queue_size()
            port = 6379  # TODO: randomise
            maxmemory = "4096mb"  # TODO: compute maxmemory from qsize
            executor, shutdown = celery_environment.launch_celery_worker_environment(
                task_desc=task_desc,
                redis_params=dict(port=port, maxmemory=maxmemory),
                workers_per_node=self._workers_per_node
            )
            return (executor, qsize, shutdown)

        def mk_dask(task_desc: TaskDescription):
            qsize = 100
            executor = _get_distributed_executor(self._opts)
            return (executor, qsize, noop)

        def mk_celery(task_desc: TaskDescription):
            qsize = 100
            executor = mk_celery_executor(*self._opts)
            return (executor, qsize, noop)

        def mk_multiproc(task_desc: TaskDescription):
            qsize = 100
            executor = _get_concurrent_executor(self._opts)
            return (executor, qsize, noop)

        def mk_serial(task_desc: TaskDescription):
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
             self._shutdown) = mk.get(self._kind, mk_serial)(task_desc)
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

    def __call__(self, task_desc: TaskDescription, tasks, run_task, on_task_complete=None):
        if self._executor is None:
            if self.start(task_desc) is False:
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
        self.workers_per_node: int = None


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

    def set_workers_per_node(ctx, param, value):
        if value is None:
            return
        state(ctx).workers_per_node = value

    def capture_qsub(ctx, param, value):
        if value is None:
            return
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
            click.option('--workers-per-node',
                         type=int,
                         expose_value=False,
                         callback=set_workers_per_node,
                         help='For code that parallelizes over cores'),
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

            if s.qsub is not None and s.workers_per_node is not None:
                s.qsub.add_internal_args('--workers-per-node',
                                         str(s.workers_per_node))

            if s.runner is not None and s.workers_per_node is not None:
                s.runner.set_workers_per_node(s.workers_per_node)

        def extract_runner(*args, **kwargs):
            finalise_state()
            kwargs.update({arg_name: state().runner})
            return f(*args, **kwargs)

        return update_wrapper(extract_runner, f)

    return decorate
