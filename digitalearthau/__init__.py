from pathlib import Path

from ._version import get_versions

__version__ = get_versions()['version']
del get_versions

# TODO: Distinguish between instances: write this var during module build?
MODULE_NAME = 'dea-prod/{}'.format(__version__)

BASE_DIR = Path(__file__).absolute().parent
SCRIPT_DIR = BASE_DIR
CONFIG_DIR = BASE_DIR / 'config'
