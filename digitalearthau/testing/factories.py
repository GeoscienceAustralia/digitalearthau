"""
Methods for creating custom datacube/dea instance pytest fixtures.
"""
import logging
import pytest
from contextlib import contextmanager

import digitalearthau
import digitalearthau.system
from datacube.config import LocalConfig
from datacube.index import Index
from datacube.drivers.postgres import PostgresDb
from datacube.drivers.postgres import _dynamic
from datacube.drivers.postgres import _core

# These are unavoidable in pytests due to fixtures
# pylint: disable=redefined-outer-name,protected-access,invalid-name


def remove_dynamic_indexes():
    """
    Clear any dynamically created indexes from the schema.
    """
    # Our normal indexes start with "ix_", dynamic indexes with "dix_"
    for table in _core.METADATA.tables.values():
        table.indexes.intersection_update(
            [i for i in table.indexes if not i.name.startswith('dix_')])


@contextmanager
def _increase_logging(log, level=logging.WARN):
    previous_level = log.getEffectiveLevel()
    log.setLevel(level)
    yield
    log.setLevel(previous_level)


def db_fixture(config_fixture_name, scope='function'):
    """
    Factory to create a pytest fixture for a Datacube PostgresDb
    """

    @pytest.fixture(scope=scope)
    def db_fixture_instance(request):
        local_config: LocalConfig = request.getfixturevalue(config_fixture_name)
        db = PostgresDb.from_config(local_config,
                                    application_name='dea-test-run',
                                    validate_connection=False)
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

    return db_fixture_instance


def index_fixture(db_fixture_name, scope='function'):
    """
    Factory to create a pytest fixture for a Datacube index
    """

    @pytest.fixture(scope=scope)
    def index_fixture_instance(request):
        db: PostgresDb = request.getfixturevalue(db_fixture_name)
        return Index(db)

    return index_fixture_instance


def dea_index_fixture(index_fixture_name, scope='function'):
    """
    Create a pytest fixture for a Datacube instance populated
    with DEA products/config.
    """

    @pytest.fixture(scope=scope)
    def dea_index_instance(request):
        """
        An index initialised with DEA config (products)
        """
        index: Index = request.getfixturevalue(index_fixture_name)
        # Add DEA metadata types, products. They'll be validated too.
        digitalearthau.system.init_dea(
            index,
            with_permissions=False,
            # No "product added" logging as it makes test runs too noisy
            log_header=lambda *s: None,
            log=lambda *s: None,

        )
        return index

    return dea_index_instance
