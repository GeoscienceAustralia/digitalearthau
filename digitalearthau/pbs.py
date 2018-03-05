import uuid
from base64 import b64encode
import os
import subprocess
import platform
import re
import functools
import shlex
from collections import namedtuple, OrderedDict
from typing import Optional

Node = namedtuple('Node', ['name', 'num_cores', 'offset', 'is_main'])


def hostname():
    return platform.node()


def is_under_pbs():
    return 'PBS_NODEFILE' in os.environ


def current_pbs_job_id() -> Optional[str]:
    return os.environ.get('PBS_JOBID')


def parse_nodes_file(fname=None):
    if fname is None:
        fname = os.environ.get('PBS_NODEFILE')
        if fname is None:
            raise RuntimeError("Can't find PBS node file")

    def load_lines(fname):
        with open(fname, 'r') as f:
            ll = [l.strip() for l in f.readlines()]
            return [l for l in ll if len(l) > 0]

    main_hostname = hostname()
    _nodes = OrderedDict()

    for idx, l in enumerate(load_lines(fname)):
        if l in _nodes:
            _nodes[l]['num_cores'] += 1
        else:
            _nodes[l] = dict(
                name=l,
                num_cores=1,
                offset=idx,
                is_main=(main_hostname == l))

    return [Node(**x) for x in _nodes.values()]


# This is defined in document "DEA Event structure", to produce stable & consistent task ids for pbs jobs.
# https://docs.google.com/document/d/1VNpK3GL1r4kbjwAO-sJ6_BMk2FSHhNnoDg4VHeylyAE/edit?usp=sharing
NCI_PBS_UUID_NAMESPACE = uuid.UUID('85d36430-538f-4ecd-85d0-d0ef9edfc266')


def current_job_task_id() -> Optional[uuid.UUID]:
    """
    Get a stable UUID for the current PBS job, or nothing if we don't appear to be in one.

    >>> import mock
    >>> with mock.patch.dict(os.environ, {'PBS_JOBID': '87654321.r-man2'}):
    ...     current_job_task_id()
    UUID('9f682e52-6c9e-5ed1-a32f-1cb32f35e476')
    """
    pbs_job_id = current_pbs_job_id()
    if pbs_job_id is None:
        return None

    task_id = task_id_for_pbs_job(pbs_job_id)

    return task_id


def task_id_for_pbs_job(pbs_job_id: str) -> uuid.UUID:
    """
    Get a stable UUID for the the given PBS job id. Expects the whole job name ("8894425.r-man2"), not just the number).

    >>> task_id_for_pbs_job('7818401.r-man2')
    UUID('f3f5ab5c-ada9-5507-b00b-ad856743bb76')
    """
    # Sanity check
    if ".r-man" not in pbs_job_id:
        raise RuntimeError(
            "%r doesn't look like an NCI pbs job name. Expecting the full name, eg '8894425.r-man2'" % pbs_job_id
        )
    task_id = uuid.uuid5(NCI_PBS_UUID_NAMESPACE, pbs_job_id.strip())
    return task_id


@functools.lru_cache(maxsize=None)
def nodes():
    return parse_nodes_file()


def total_cores():
    total = 0
    for n in nodes():
        total += n.num_cores
    return total


def preferred_queue_size():
    return total_cores() * 2


def get_env(extras=None, **more_env):
    extras = extras or []

    pass_envs = set(['PATH', 'LANG', 'LD_LIBRARY_PATH', 'HOME', 'USER',
                     'CPL_ZIP_ENCODING'])
    regexes = ['^PYTHON.*', '^GDAL.*', '^LC.*', '^DATACUBE.*']
    rgxs = [re.compile(r) for r in regexes]

    def need_this_env(k):
        if k in pass_envs or k in extras:
            return True
        for rgx in rgxs:
            if rgx.match(k):
                return True
        return False

    ee = dict((k, v) for k, v in os.environ.items() if need_this_env(k))
    ee.update(**more_env)
    return ee


def mk_exports(env):
    return '\n'.join('export {}={}'.format(k, shlex.quote(v)) for k, v in env.items())


def generate_env_header(extras=None, **more_env):
    return mk_exports(get_env(extras, **more_env))


def wrap_script(script):
    b64s = b64encode(script.encode('ascii')).decode('ascii')
    return 'eval "$(echo {}|base64 --decode)"'.format(b64s)


def pbsdsh(cpu_num, script, env=None, test_mode=False) -> subprocess.Popen:

    if env is None:
        env = get_env()

    hdr = mk_exports(env) + '\n\n'

    if test_mode:
        args = "env -i bash --norc -c".split(' ')
    else:
        args = "pbsdsh -n {} -- bash -c".format(cpu_num).split(' ')

    args.append(wrap_script(hdr + script))
    return subprocess.Popen(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
