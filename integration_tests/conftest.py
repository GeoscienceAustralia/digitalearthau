import itertools
import logging
import os
import shutil
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Tuple, NamedTuple, Optional

import pytest
import yaml

import digitalearthau
import digitalearthau.system
from datacube.config import LocalConfig
from datacube.index._api import Index
from datacube.index.postgres import PostgresDb
from datacube.index.postgres import _dynamic
from datacube.index.postgres.tables import _core
from digitalearthau import paths, collections
from digitalearthau.collections import Collection
from digitalearthau.index import DatasetLite, AgdcDatasetPathIndex
from digitalearthau.paths import register_base_directory

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
    # Add DEA metadata types, products. They'll be validated too.
    digitalearthau.system.init_dea(
        index,
        with_permissions=False,
        # No "product added" logging as it makes test runs too noisy
        log_header=lambda *s: None,
        log=lambda *s: None,

    )

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


ON_DISK2_ID = DatasetLite(uuid.UUID('10c4a9fe-2890-11e6-8ec8-a0000100fe80'))

ON_DISK2_OFFSET = ('LS8_OLITIRS_OTH_P51_GALPGS01-032_114_080_20150924', 'ga-metadata.yaml')


class DatasetOnDisk(NamedTuple):
    """
    Information on a test dataset. The properties are recorded here separately so tests can verify them.
    """
    collection: Optional[Collection]

    id_: uuid.UUID

    # We separate path from a test base path for calculating trash prefixes etc.
    # You usually just want to use `self.path` instead.
    base_path: Path
    path_offset: Tuple[str, ...]

    # Source dataset that will be indexed if this is indexed (ie. embedded inside it)
    parent_id: uuid.UUID = None

    @property
    def path(self):
        return self.base_path.joinpath(*self.path_offset)

    @property
    def uri(self):
        return self.path.as_uri()

    @property
    def dataset(self):
        return DatasetLite(self.id_)

    @property
    def parent(self) -> Optional[DatasetLite]:
        """Source datasets that will be indexed if on_disk1 is indexed"""
        return DatasetLite(self.parent_id) if self.parent_id else None


# We want one fixture to return all of this data. Returning a tuple was getting unwieldy.
class SimpleEnv(NamedTuple):
    collection: Collection

    on_disk1_id: uuid.UUID
    on_disk_uri: str

    base_test_path: Path


@pytest.fixture
def test_dataset(integration_test_data, dea_index) -> DatasetOnDisk:
    """A dataset on disk, indexed in a collection"""
    test_data = integration_test_data

    # Tests assume one dataset for the collection, so delete the second.
    shutil.rmtree(str(test_data.joinpath('LS8_OLITIRS_OTH_P51_GALPGS01-032_114_080_20150924')))
    index = AgdcDatasetPathIndex(dea_index)
    ls8_collection = Collection(
        name='ls8_scene_test',
        query={},
        file_patterns=[str(test_data.joinpath('LS8*/ga-metadata.yaml'))],
        unique=[],
        index=index
    )
    collections._add(ls8_collection)

    # Add a decoy collection.
    ls5_nc_collection = Collection(
        name='ls5_nc_test',
        query={},
        file_patterns=[str(test_data.joinpath('LS5*.nc'))],
        unique=[],
        index=index
    )
    collections._add(ls8_collection)

    # register this as a base directory so that datasets can be trashed within it.
    register_base_directory(str(test_data))

    cache_path = test_data.joinpath('cache')
    cache_path.mkdir()

    return DatasetOnDisk(
        collection=ls8_collection,
        id_=uuid.UUID('86150afc-b7d5-4938-a75e-3445007256d3'),
        base_path=test_data,
        path_offset=('LS8_OLITIRS_OTH_P51_GALPGS01-032_114_080_20160926', 'ga-metadata.yaml'),
        parent_id=uuid.UUID('dee471ed-5aa5-46f5-96b5-1e1ea91ffee4')
    )


@pytest.fixture
def other_dataset(integration_test_data: Path) -> DatasetOnDisk:
    """
    A dataset matching the same collection as test_dataset, but not indexed.
    """

    ds_id = uuid.UUID("5294efa6-348d-11e7-a079-185e0f80a5c0")
    paths.write_files(
        {
            'LS8_INDEXED_ALREADY': {
                'ga-metadata.yaml': ("""
id: %s
platform:
    code: LANDSAT_8
instrument:
    name: OLI_TIRS
format:
    name: GeoTIFF
product_type: level1
product_level: L1T
image:
    bands: {}
lineage:
    source_datasets: {}
    """ % str(ds_id)),
                'dummy-file.txt': ''
            }
        },
        containing_dir=integration_test_data
    )

    return DatasetOnDisk(
        collection=None,
        id_=ds_id,
        base_path=integration_test_data,
        path_offset=('LS8_INDEXED_ALREADY', 'ga-metadata.yaml')
    )
