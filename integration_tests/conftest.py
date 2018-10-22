import shutil
import uuid
from datetime import datetime
from pathlib import Path
from textwrap import dedent
from typing import Tuple, NamedTuple, Optional, Mapping, Iterable

import pytest
import structlog
import yaml
from sqlalchemy import and_

import digitalearthau.system
from datacube.index import Index
from datacube.drivers.postgres import _api
from datacube.model import Dataset
from digitalearthau import paths, collections
from digitalearthau.collections import Collection
from digitalearthau.index import DatasetLite, add_dataset
from digitalearthau.paths import register_base_directory
from digitalearthau.uiutil import CleanConsoleRenderer

try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import SafeLoader

pytest_plugins = "digitalearthau.testing.plugin"

INTEGRATION_DEFAULT_CONFIG_PATH = Path(__file__).parent.joinpath('deaintegration.conf')

INTEGRATION_TEST_DATA = Path(__file__).parent / 'data'

PROJECT_ROOT = Path(__file__).parents[1]

DEA_MD_TYPES = digitalearthau.CONFIG_DIR / 'metadata-types.yaml'
DEA_PRODUCTS_DIR = digitalearthau.CONFIG_DIR / 'products'


@pytest.fixture(scope="session", autouse=True)
def configure_log_output(request):
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M.%S"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            # Coloured output if to terminal.
            CleanConsoleRenderer()
        ],
        context_class=dict,
        cache_logger_on_first_use=True,
    )


def load_yaml_file(path):
    with path.open() as f:
        return list(yaml.load_all(f, Loader=SafeLoader))


@pytest.fixture(autouse=True)
def work_path(tmpdir):
    paths.NCI_WORK_ROOT = Path(tmpdir) / 'work'
    paths.NCI_WORK_ROOT.mkdir()
    # The default use of timestamp will collide when run quickly, as in unit tests.
    paths._JOB_WORK_OFFSET = '{output_product}-{task_type}-{request_uuid}'
    return paths.NCI_WORK_ROOT


@pytest.fixture
def integration_test_data(tmpdir):
    temp_data_dir = Path(tmpdir) / 'integration_data'
    shutil.copytree(INTEGRATION_TEST_DATA, temp_data_dir)
    return temp_data_dir


ON_DISK2_ID = DatasetLite(uuid.UUID('10c4a9fe-2890-11e6-8ec8-a0000100fe80'))

ON_DISK2_OFFSET = ('LS8_OLITIRS_OTH_P51_GALPGS01-032_114_080_20150924', 'ga-metadata.yaml')


class DatasetForTests(NamedTuple):
    """
    A test dataset, including the file location and collection it should belong to.

    When your test starts the dataset will be on disk but not yet indexed. Call add_to_index() and others as needed.

    All properties are recorded here separately so tests can verify them independently.
    """
    # The test collection this should belong to
    collection: Collection

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
    def copyable_path(self):
        """Get the path containing the whole dataset that can be copied on disk.

        The recorded self.path of datasets is the path to the metadata, but "packaged" datasets
        such as scenes have a folder hierarchy, and to copy them we want to copy the whole scene
        folder, not just the metadata file.

        (This will return a folder for a scene, and will be identical to self.path for typical NetCDFs)
        """
        package_path, _ = paths.get_dataset_paths(self.path)
        return package_path

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

    def add_to_index(self):
        """Add to the current collection's index"""
        add_dataset(self.collection.index_, self.id_, self.uri)

    def archive_in_index(self, archived_dt: datetime = None):
        archive_dataset(self.id_, self.collection, archived_dt=archived_dt)

    def archive_location_in_index(self, archived_dt: datetime = None, uri: str = None):
        archive_location(self.id_, uri or self.uri, self.collection, archived_dt=archived_dt)

    def add_location(self, uri: str) -> bool:
        return self.collection.index_.datasets.add_location(self.id_, uri)

    def get_index_record(self) -> Optional[Dataset]:
        """If this is indexed, return the full Dataset record"""
        return self.collection.index_.datasets.get(self.id_)


# We want one fixture to return all of this data. Returning a tuple was getting unwieldy.
class SimpleEnv(NamedTuple):
    collection: Collection

    on_disk1_id: uuid.UUID
    on_disk_uri: str

    base_test_path: Path


@pytest.fixture
def test_dataset(integration_test_data, dea_index) -> DatasetForTests:
    """A dataset on disk, with corresponding collection"""
    test_data = integration_test_data

    # Tests assume one dataset for the collection, so delete the second.
    shutil.rmtree(str(test_data.joinpath('LS8_OLITIRS_OTH_P51_GALPGS01-032_114_080_20150924')))
    ls8_collection = Collection(
        name='ls8_scene_test',
        query={},
        file_patterns=[str(test_data.joinpath('LS8*/ga-metadata.yaml'))],
        unique=[],
        index_=dea_index
    )
    collections._add(ls8_collection)

    # Add a decoy collection.
    ls5_nc_collection = Collection(
        name='ls5_nc_test',
        query={},
        file_patterns=[str(test_data.joinpath('LS5*.nc'))],
        unique=[],
        index_=dea_index
    )
    collections._add(ls5_nc_collection)

    # register this as a base directory so that datasets can be trashed within it.
    register_base_directory(str(test_data))

    cache_path = test_data.joinpath('cache')
    cache_path.mkdir()

    return DatasetForTests(
        collection=ls8_collection,
        id_=uuid.UUID('86150afc-b7d5-4938-a75e-3445007256d3'),
        base_path=test_data,
        path_offset=('LS8_OLITIRS_OTH_P51_GALPGS01-032_114_080_20160926', 'ga-metadata.yaml'),
        parent_id=uuid.UUID('dee471ed-5aa5-46f5-96b5-1e1ea91ffee4')
    )


@pytest.fixture
def other_dataset(integration_test_data: Path, test_dataset: DatasetForTests) -> DatasetForTests:
    """
    A dataset matching the same collection as test_dataset, but not indexed.
    """

    ds_id = uuid.UUID("5294efa6-348d-11e7-a079-185e0f80a5c0")
    paths.write_files(
        {
            'LS8_INDEXED_ALREADY': {
                'ga-metadata.yaml': (dedent("""\
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
                                         source_datasets: {}""" % str(ds_id))),
                'dummy-file.txt': ''
            }
        },
        containing_dir=integration_test_data
    )

    return DatasetForTests(
        collection=test_dataset.collection,
        id_=ds_id,
        base_path=integration_test_data,
        path_offset=('LS8_INDEXED_ALREADY', 'ga-metadata.yaml')
    )


def archive_dataset(dataset_id: uuid.UUID, collection: Collection, archived_dt: datetime = None):
    if archived_dt is None:
        collection.index_.datasets.archive([dataset_id])
    else:
        # Hack until ODC allows specifying the archive time.
        with collection.index_._db.begin() as transaction:
            # SQLAlchemy queries require "column == None", not "column is None" due to operator overloading:
            # pylint: disable=singleton-comparison
            transaction._connection.execute(
                _api.DATASET.update().where(
                    _api.DATASET.c.id == dataset_id
                ).where(
                    _api.DATASET.c.archived == None
                ).values(
                    archived=archived_dt
                )
            )


def archive_location(dataset_id: uuid.UUID, uri: str, collection: Collection, archived_dt: datetime = None):
    if archived_dt is None:
        collection.index_.datasets.archive_location(dataset_id, uri)
    else:
        scheme, body = _api._split_uri(uri)
        # Hack until ODC allows specifying the archive time.
        with collection.index_._db.begin() as transaction:
            # SQLAlchemy queries require "column == None", not "column is None" due to operator overloading:
            # pylint: disable=singleton-comparison
            transaction._connection.execute(
                _api.DATASET_LOCATION.update().where(
                    and_(
                        _api.DATASET_LOCATION.c.dataset_ref == dataset_id,
                        _api.DATASET_LOCATION.c.uri_scheme == scheme,
                        _api.DATASET_LOCATION.c.uri_body == body,
                        _api.DATASET_LOCATION.c.archived == None,
                    )
                ).values(
                    archived=archived_dt
                )
            )


def freeze_index(index: Index) -> Mapping[DatasetLite, Iterable[str]]:
    """
    All contained (dataset_id, [location]) values, to check test results.
    """
    return dict(
        (
            DatasetLite(dataset.id, archived_time=dataset.archived_time),
            tuple(dataset.uris)
        )
        for dataset in index.datasets.search()
    )
