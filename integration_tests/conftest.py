import itertools
import logging
import os
from contextlib import contextmanager
from pathlib import Path

import pytest
import shutil
import yaml

import digitalearthau
from datacube.config import LocalConfig
from datacube.index._api import Index
from datacube.index.postgres import PostgresDb
from datacube.index.postgres import _dynamic
from datacube.index.postgres.tables import _core

try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import SafeLoader

INTEGRATION_DEFAULT_CONFIG_PATH = Path(__file__).parent.joinpath('deaintegration.conf')

INTEGRATION_TEST_DATA = Path(__file__).parent / 'data'

PROJECT_ROOT = Path(__file__).parents[1]

DEA_MD_TYPES = digitalearthau.CONFIG_DIR / 'metadata-types.yaml'
DEA_PRODUCTS_DIR = digitalearthau.CONFIG_DIR / 'products'


def load_yaml_file(path):
    with path.open() as f:
        return list(yaml.load_all(f, Loader=SafeLoader))


@pytest.fixture
def integration_test_data(tmpdir):
    d = tmpdir.join('integration_data')
    shutil.copytree(str(INTEGRATION_TEST_DATA), str(d))
    return Path(str(d))


@pytest.fixture
def dea_index(index: Index):
    """
    An index initialised with DEA config (products)
    """
    # Add DEA metadata types, products. They'll be validated etc.
    for md_type_def in load_yaml_file(DEA_MD_TYPES):
        index.metadata_types.add(index.metadata_types.from_doc(md_type_def))

    for product_file in DEA_PRODUCTS_DIR.glob('*.yaml'):
        for product_def in load_yaml_file(product_file):
            index.products.add_document(product_def)

    return index


@pytest.fixture
def datasets(dea_index):
    # Add test datasets, collection definitions.
    pass


@pytest.fixture
def integration_config_paths():
    return (
        str(INTEGRATION_DEFAULT_CONFIG_PATH),
        os.path.expanduser('~/.datacube_integration.conf')
    )


@pytest.fixture
def global_integration_cli_args(integration_config_paths):
    """
    The first arguments to pass to a cli command for integration test configuration.
    """
    # List of a config files in order.
    return list(itertools.chain(*(('--config_file', f) for f in integration_config_paths)))


@pytest.fixture
def local_config(integration_config_paths):
    return LocalConfig.find(integration_config_paths)


@pytest.fixture()
def db(local_config):
    db = PostgresDb.from_config(local_config, application_name='dea-test-run', validate_connection=False)

    # Drop and recreate tables so our tests have a clean db.
    with db.connect() as connection:
        _core.drop_db(connection._connection)
    remove_dynamic_indexes()

    # Disable informational messages since we're doing this on every test run.
    with _increase_logging(_core._LOG) as _:
        _core.ensure_db(db._engine)

    # We don't need informational create/drop messages for every config change.
    _dynamic._LOG.setLevel(logging.WARN)

    yield db
    db.close()


@contextmanager
def _increase_logging(log, level=logging.WARN):
    previous_level = log.getEffectiveLevel()
    log.setLevel(level)
    yield
    log.setLevel(previous_level)


def remove_dynamic_indexes():
    """
    Clear any dynamically created indexes from the schema.
    """
    # Our normal indexes start with "ix_", dynamic indexes with "dix_"
    for table in _core.METADATA.tables.values():
        table.indexes.intersection_update([i for i in table.indexes if not i.name.startswith('dix_')])


@pytest.fixture
def index(db):
    """
    :type db: datacube.index.postgres._api.PostgresDb
    """
    return Index(db)
