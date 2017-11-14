import itertools
import logging
import os
from contextlib import contextmanager
from pathlib import Path

import pytest

import digitalearthau
import digitalearthau.system
from datacube.config import LocalConfig
from datacube.index._api import Index
from datacube.index.postgres import PostgresDb
from datacube.index.postgres import _dynamic
from datacube.index.postgres.tables import _core

# These are unavoidable in pytests due to fixtures
# pylint: disable=redefined-outer-name,protected-access,invalid-name

try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import SafeLoader

# The default test config options.
# The user overrides these by creating their own file in ~/.datacube_integration.conf
INTEGRATION_DEFAULT_CONFIG_PATH = Path(__file__).parent.joinpath('testing-default.conf')


@pytest.fixture
def integration_config_paths():
    if not INTEGRATION_DEFAULT_CONFIG_PATH.exists():
        # Safety check. We never want it falling back to the default config,
        # as it will alter/wipe the user's own datacube to run tests
        raise RuntimeError('Integration default file not found. This should be built-in?')

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


def remove_dynamic_indexes():
    """
    Clear any dynamically created indexes from the schema.
    """
    # Our normal indexes start with "ix_", dynamic indexes with "dix_"
    for table in _core.METADATA.tables.values():
        table.indexes.intersection_update([i for i in table.indexes if not i.name.startswith('dix_')])


@contextmanager
def _increase_logging(log, level=logging.WARN):
    previous_level = log.getEffectiveLevel()
    log.setLevel(level)
    yield
    log.setLevel(previous_level)


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


@pytest.fixture
def index(db):
    """
    :type db: datacube.index.postgres._api.PostgresDb
    """
    return Index(db)


@pytest.fixture
def dea_index(index: Index):
    """
    An index initialised with DEA config (products)
    """
    # Add DEA metadata types, products. They'll be validated too.
    digitalearthau.system.init_dea(
        index,
        with_permissions=False,
        # No "product added" logging as it makes test runs too noisy
        log_header=lambda *s: None,
        log=lambda *s: None,

    )

    return index
