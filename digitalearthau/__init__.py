from pathlib import Path
import re

from ._version import get_versions

__version__ = get_versions()['version']
del get_versions

match = re.search(r'modules/(.*)/lib', __file__).group(1)
if match is not None:
    MODULE_NAME = match.group(1)
else:
    MODULE_NAME = 'not_installed_in_a_module'
del match


BASE_DIR = Path(__file__).absolute().parent
SCRIPT_DIR = BASE_DIR
CONFIG_DIR = BASE_DIR / 'config'
INGEST_CONFIG_DIR = CONFIG_DIR / 'ingestion'
