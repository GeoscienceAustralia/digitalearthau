from pathlib import Path
import re

from ._version import get_versions

__version__ = get_versions()['version']
del get_versions


def _get_module_name():
    _match = re.search(r'modules/(.*)/lib', __file__)
    if _match is not None:
        return _match.group(1)
    else:
        return 'not_installed_in_a_module'


MODULE_NAME = _get_module_name()

BASE_DIR = Path(__file__).absolute().parent
SCRIPT_DIR = BASE_DIR
CONFIG_DIR = BASE_DIR.parent / 'config' / 'config' / 'collection-2-nci'
INGEST_CONFIG_DIR = CONFIG_DIR / 'ingestion'
PRODUCTS_CONFIG_DIR = CONFIG_DIR / 'products'
EO3_CONFIG_DIR = CONFIG_DIR / 'config' / 'config' / 'collection-3-nci'
