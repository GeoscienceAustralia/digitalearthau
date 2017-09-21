from base64 import b64encode
import os
import subprocess
import platform
import re
import functools
import shlex
from collections import namedtuple, OrderedDict

Node = namedtuple('Node', ['name', 'num_cores', 'offset', 'is_main'])


def hostname():
    return platform.node()


def is_under_pbs():
    return 'PBS_NODEFILE' in os.environ


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

    pass_envs = set(['PATH', 'LANG', 'LD_LIBRARY_PATH', 'HOME', 'USER'])
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


def pbsdsh(cpu_num, script, env=None, test_mode=False):

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
        stderr=subprocess.PIPE)
