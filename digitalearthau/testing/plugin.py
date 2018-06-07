import itertools

import os
import pytest
from pathlib import Path
from typing import Iterable

import datacube
import digitalearthau
import digitalearthau.system
from datacube.config import LocalConfig
from . import factories as factories

# These are unavoidable in pytests due to fixtures
# pylint: disable=redefined-outer-name,protected-access,invalid-name

try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import SafeLoader

# The default test config options.
# The user overrides these by creating their own file in ~/.datacube_integration.conf
INTEGRATION_DEFAULT_CONFIG_PATH = Path(__file__).parent.joinpath('testing-default.conf')


def pytest_report_header(config):
    if config.getoption('verbose') > 0:
        return (
            f"digitaleathau {digitalearthau.__version__}, "
            f"opendatacube {datacube.__version__}"
        )

    return None


@pytest.fixture(scope='session')
def integration_config_paths():
    if not INTEGRATION_DEFAULT_CONFIG_PATH.exists():
        # Safety check. We never want it falling back to the default config,
        # as it will alter/wipe the user's own datacube to run tests
        raise RuntimeError(
            'Integration default file not found. This should be built-in?')

    return (
        str(INTEGRATION_DEFAULT_CONFIG_PATH),
        os.path.expanduser('~/.datacube_integration.conf')
    )


@pytest.fixture(scope='session')
def global_integration_cli_args(integration_config_paths: Iterable[str]):
    """
    The first arguments to pass to a cli command for integration test configuration.
    """
    # List of a config files in order.
    return list(
        itertools.chain(*(('--config_file', f) for f in integration_config_paths)))


@pytest.fixture(scope='session')
def local_config(integration_config_paths):
    return LocalConfig.find(integration_config_paths)


# Default fixtures which will drop/create on every individual test function.
db = factories.db_fixture('local_config')
index = factories.index_fixture('db')
dea_index = factories.dea_index_fixture('index')
