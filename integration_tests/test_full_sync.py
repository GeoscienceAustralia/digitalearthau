import logging
import os
import shutil
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Mapping, Tuple

import pytest
import structlog
from dateutil import tz

from datacube.index.postgres import _api
from datacube.utils import uri_to_local_path
from digitalearthau import paths, collections
from digitalearthau.uiutil import CleanConsoleRenderer
from digitalearthau.collections import Collection
from digitalearthau.index import DatasetLite, MemoryDatasetPathIndex, AgdcDatasetPathIndex
from digitalearthau.paths import register_base_directory
from digitalearthau.sync import differences as mm, fixes, scan, Mismatch


# These are ok in tests.
# pylint: disable=too-many-locals, protected-access, redefined-outer-name


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


ON_DISK1_ID = DatasetLite(uuid.UUID('86150afc-b7d5-4938-a75e-3445007256d3'))
ON_DISK2_ID = DatasetLite(uuid.UUID('10c4a9fe-2890-11e6-8ec8-a0000100fe80'))

ON_DISK1_OFFSET = ('LS8_OLITIRS_OTH_P51_GALPGS01-032_114_080_20160926', 'ga-metadata.yaml')
ON_DISK2_OFFSET = ('LS8_OLITIRS_OTH_P51_GALPGS01-032_114_080_20150924', 'ga-metadata.yaml')

# Source datasets that will be indexed if on_disk1 is indexed
ON_DISK1_PARENT = DatasetLite(uuid.UUID('dee471ed-5aa5-46f5-96b5-1e1ea91ffee4'))


@pytest.fixture
def syncable_environment(integration_test_data, dea_index):
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

    on_disk_uri = test_data.joinpath(*ON_DISK1_OFFSET).as_uri()

    return ls8_collection, ON_DISK1_ID, on_disk_uri, Path(str(test_data))


@pytest.fixture
def other_dataset_id_path(syncable_environment):
    ls8_collection, on_disk, on_disk_uri, root = syncable_environment

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
        containing_dir=root
    )
    ds_path = root.joinpath('LS8_INDEXED_ALREADY', 'ga-metadata.yaml')
    return DatasetLite(ds_id), ds_path


def test_new_and_old_on_disk(syncable_environment, other_dataset_id_path):
    # type: (Tuple[Collection, DatasetLite, str, Path]) -> None
    ls8_collection, on_disk, on_disk_uri, root = syncable_environment
    old_indexed = DatasetLite(uuid.UUID('5294efa6-348d-11e7-a079-185e0f80a5c0'))

    # An indexed file not on disk, and disk file not in index.

    missing, missing_path = other_dataset_id_path
    missing_uri = missing_path.as_uri()
    ls8_collection._index.add_dataset(missing, missing_uri)
    # Make it missing
    shutil.rmtree(str(missing_path.parent))

    _check_sync(
        collection=ls8_collection,
        expected_paths=[
            missing_uri,
            on_disk_uri
        ],
        expected_mismatches=[
            mm.LocationMissingOnDisk(old_indexed, missing_path.as_uri()),
            mm.DatasetNotIndexed(on_disk, on_disk_uri)
        ],
        expected_index_result={
            on_disk: (on_disk_uri,),
            old_indexed: (),
            ON_DISK1_PARENT: (),
        },
        cache_path=root,
        fix_settings=dict(index_missing=True, update_locations=True)
    )


def test_replace_on_disk(syncable_environment, other_dataset_id_path):
    # type: (Tuple[Collection, DatasetLite, str, Path]) -> None
    """
    File on disk has a different id to the one in the index (ie. it was quietly reprocessed)
    """
    ls8_collection, on_disk, on_disk_uri, root = syncable_environment

    other, other_path = other_dataset_id_path
    ls8_collection._index.add_dataset(on_disk, on_disk_uri)

    # move a new one over the top
    shutil.move(other_path, str(uri_to_local_path(on_disk_uri)))

    _check_sync(
        collection=ls8_collection,
        expected_paths=[
            on_disk_uri
        ],
        expected_mismatches=[
            mm.LocationMissingOnDisk(on_disk, on_disk_uri),
            mm.DatasetNotIndexed(other, on_disk_uri),
        ],
        expected_index_result={
            on_disk: (),
            other: (on_disk_uri,),
            ON_DISK1_PARENT: (),
        },
        cache_path=root,
        fix_settings=dict(index_missing=True, update_locations=True)
    )


def test_move_on_disk(syncable_environment, other_dataset_id_path):
    # type: (Tuple[Collection, DatasetLite, str, Path]) -> None
    """
    Indexed dataset was moved over the top of another indexed dataset
    """
    ls8_collection, on_disk, on_disk_uri, root = syncable_environment

    other, other_path = other_dataset_id_path
    ls8_collection._index.add_dataset(on_disk, on_disk_uri)
    ls8_collection._index.add_dataset(other, other_path.as_uri())

    shutil.move(other_path, str(uri_to_local_path(on_disk_uri)))

    _check_sync(
        collection=ls8_collection,
        expected_paths=[
            on_disk_uri,
            other_path.as_uri(),
        ],
        expected_mismatches=[
            mm.LocationMissingOnDisk(on_disk, on_disk_uri),
            mm.LocationNotIndexed(other, on_disk_uri),
            mm.LocationMissingOnDisk(other, other_path.as_uri()),
        ],
        expected_index_result={
            on_disk: (),
            other: (on_disk_uri,),
            ON_DISK1_PARENT: (),
        },
        cache_path=root,
        fix_settings=dict(index_missing=True, update_locations=True)
    )


def test_archived_on_disk(syncable_environment):
    # type: (Tuple[Collection, DatasetLite, str, Path]) -> None
    """
    A an already-archived dataset on disk. Should report it, but not touch the file (trash_archived is false)
    """
    ls8_collection, on_disk, on_disk_uri, root = syncable_environment

    # archived_on_disk = DatasetLite(on_disk.id, archived_time=(datetime.utcnow() - timedelta(days=5)))
    ls8_collection._index.add_dataset(on_disk, on_disk_uri)
    ls8_collection._index._index.datasets.archive([on_disk.id])
    archived_time = ls8_collection._index._index.datasets.get(on_disk.id).archived_time

    assert uri_to_local_path(on_disk_uri).exists(), "On-disk location should exist before test begins."
    _check_sync(
        collection=ls8_collection,
        expected_paths=[
            on_disk_uri
        ],
        expected_mismatches=[
            mm.ArchivedDatasetOnDisk(DatasetLite(on_disk.id, archived_time), on_disk_uri),
        ],
        expected_index_result={
            # Not active in index, as it's archived.
            # on_disk: (on_disk_uri,),
            # But the parent dataset still is:
            ON_DISK1_PARENT: (),
        },
        cache_path=root,
        fix_settings=dict(index_missing=True, update_locations=True)
    )
    assert uri_to_local_path(on_disk_uri).exists(), "On-disk location shouldn't be touched"


def test_detect_corrupt_existing(syncable_environment):
    # type: (Tuple[Collection, str, str, Path]) -> None
    """If a dataset exists but cannot be read, report as corrupt"""
    ls8_collection, on_disk, on_disk_uri, root = syncable_environment
    path = uri_to_local_path(on_disk_uri)

    ls8_collection._index.add_dataset(on_disk, on_disk_uri)
    assert path.exists()

    # Overwrite with corrupted file.
    os.unlink(str(path))
    with path.open('w') as f:
        f.write('corruption!')
    assert path.exists()

    # Another dataset exists in the same location

    _check_sync(
        collection=ls8_collection,
        expected_paths=[on_disk_uri],
        expected_mismatches=[
            # We don't know if it's the same dataset
            mm.UnreadableDataset(None, on_disk_uri)
        ],
        # Unmodified index
        expected_index_result=ls8_collection._index.as_map(),
        cache_path=root,
        fix_settings=dict(trash_missing=True, trash_archived=True, update_locations=True)
    )
    # If a dataset is in the index pointing to the corrupt location, it shouldn't be trashed with trash_archived=True
    assert path.exists(), "Corrupt dataset with sibling in index should not be trashed"


def test_detect_corrupt_new(syncable_environment):
    # type: (Tuple[Collection, str, str, Path]) -> None
    """If a dataset exists but cannot be read handle as corrupt"""
    ls8_collection, on_disk, on_disk_uri, root = syncable_environment
    path = uri_to_local_path(on_disk_uri)

    # Write corrupted file.
    os.unlink(str(path))
    with path.open('w') as f:
        f.write('corruption!')
    assert path.exists()

    # No dataset in index at the corrupt location, so it should be trashed.
    _check_sync(
        collection=ls8_collection,
        expected_paths=[on_disk_uri],
        expected_mismatches=[
            mm.UnreadableDataset(None, on_disk_uri)
        ],
        expected_index_result={},
        cache_path=root,
        fix_settings=dict(trash_missing=True, trash_archived=True, update_locations=True)
    )
    assert not path.exists(), "Corrupt dataset without sibling should be trashed with trash_archived=True"


_TRASH_PREFIX = ('.trash', (datetime.utcnow().strftime('%Y-%m-%d')))


# noinspection PyShadowingNames
def test_remove_missing(syncable_environment, other_dataset_id_path):
    """An on-disk dataset that's not indexed should be trashed when trash_missing=True"""
    ls8_collection, on_disk, on_disk_uri, root = syncable_environment

    register_base_directory(root)
    on_disk_path = root.joinpath(*ON_DISK1_OFFSET)
    trashed_path = root.joinpath(*_TRASH_PREFIX, *ON_DISK1_OFFSET)

    # Add a second dataset that's indexed. Should not be touched!
    other, other_path = other_dataset_id_path
    ls8_collection._index.add_dataset(other, other_path.as_uri())

    assert other_path.exists()

    assert on_disk_path.exists(), "On-disk location should exist before test begins."
    assert not trashed_path.exists(), "Trashed file shouldn't exit."
    _check_sync(
        collection=ls8_collection,
        expected_paths=[
            on_disk_uri,
            other_path.as_uri(),
        ],
        expected_mismatches=[
            mm.DatasetNotIndexed(on_disk, on_disk_uri)
        ],
        # Unmodified index
        expected_index_result=ls8_collection._index.as_map(),
        cache_path=root,
        fix_settings=dict(trash_missing=True, update_locations=True)
    )
    assert not on_disk_path.exists(), "On-disk location should exist before test begins."
    assert trashed_path.exists(), "Trashed file shouldn't exit."
    assert other_path.exists(), "Dataset outside of collection folder shouldn't be touched"


def now_utc():
    return datetime.utcnow().replace(tzinfo=tz.tzutc())


@pytest.mark.parametrize("archived_dt,expect_to_be_trashed", [
    # Default settings: trash files archived more than three days ago.
    # Four days ago, should be trashed.
    (now_utc() - timedelta(days=4), True),
    # Only one day ago, not trashed
    (now_utc() - timedelta(days=1), False),
    # One day in the future, not trashed.
    (now_utc() + timedelta(days=1), False),
])
def test_is_trashed(syncable_environment, archived_dt, expect_to_be_trashed):
    ls8_collection, on_disk, on_disk_uri, root = syncable_environment

    # Same test, but trash_archived=True, so it should be renamed to the.
    register_base_directory(root)
    archived_on_disk = DatasetLite(on_disk.id, archived_time=archived_dt)
    ls8_collection._index.add_dataset(archived_on_disk, on_disk_uri)
    archive_dataset(archived_dt, on_disk.id, ls8_collection)

    on_disk_path = root.joinpath(*ON_DISK1_OFFSET)

    trashed_path = root.joinpath(*_TRASH_PREFIX, *ON_DISK1_OFFSET)
    # Before the test, file is in place and nothing trashed.
    assert on_disk_path.exists(), "On-disk location should exist before test begins."
    assert not trashed_path.exists(), "Trashed file shouldn't exit."
    _check_sync(
        collection=ls8_collection,
        expected_paths=[
            on_disk_uri
        ],
        expected_mismatches=[
            mm.ArchivedDatasetOnDisk(archived_on_disk, on_disk_uri),
        ],
        expected_index_result={
            # Archived: shouldn't be active in index.
            # on_disk: (on_disk_uri,),
            # Prov parent should still exist as it wasn't archived.
            ON_DISK1_PARENT: (),
        },
        cache_path=root,
        fix_settings=dict(index_missing=True, update_locations=True, trash_archived=True)
    )

    # Show output structure for debugging
    print("Output structure")
    for p in paths.list_file_paths(root):
        print("\t{}".format(p))

    if expect_to_be_trashed:
        assert trashed_path.exists(), "File isn't in trash."
        assert not on_disk_path.exists(), "On-disk location still exists (should have been moved to trash)."
    else:
        assert not trashed_path.exists(), "File shouldn't have been trashed."
        assert on_disk_path.exists(), "On-disk location should still be in place."


def archive_dataset(archived_dt, dataset_id, collection):
    # Hack until ODC allows specifying the archive time.
    with collection._index._index._db.begin() as transaction:
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


def _check_sync(expected_paths: Iterable[str],
                collection: Collection,
                expected_mismatches: Iterable[Mismatch],
                expected_index_result: Mapping[DatasetLite, Iterable[str]],
                cache_path: Path,
                fix_settings: dict):
    """Check the correct outputs come from the given sync inputs"""
    log = structlog.getLogger()

    cache_path = cache_path.joinpath(str(uuid.uuid4()))
    cache_path.mkdir()

    _check_pathset_loading(cache_path, expected_paths, log, collection)

    mismatches = _check_mismatch_find(cache_path, expected_mismatches, collection._index, log, collection)

    _check_mismatch_fix(collection._index, mismatches, expected_index_result, fix_settings=fix_settings)


# noinspection PyProtectedMember
def _check_pathset_loading(cache_path: Path,
                           expected_paths: Iterable[str],
                           log: logging.Logger,
                           collection: Collection):
    """Check that the right mix of paths (index and filesystem) are loaded"""
    path_set = scan.build_pathset(collection, cache_path, log=log)

    loaded_paths = set(path_set.iterkeys('file://'))
    assert loaded_paths == set(expected_paths)

    # Sanity check that a random path doesn't match...
    dummy_dataset = cache_path.joinpath('dummy_dataset', 'ga-metadata.yaml')
    assert dummy_dataset.absolute().as_uri() not in path_set


def _check_mismatch_find(cache_path, expected_mismatches, index, log, collection: Collection):
    """Check that the correct mismatches were found"""

    mismatches = []

    for mismatch in scan.mismatches_for_collection(collection, cache_path, index):
        print(repr(mismatch))
        mismatches.append(mismatch)

    def mismatch_sort_key(m):
        dataset_id = None
        if m.dataset:
            dataset_id = m.dataset.id
        return m.__class__.__name__, dataset_id, m.uri

    sorted_mismatches = sorted(mismatches, key=mismatch_sort_key)
    sorted_expected_mismatches = sorted(expected_mismatches, key=mismatch_sort_key)

    assert sorted_mismatches == sorted_expected_mismatches

    # DatasetLite.__eq__ only tests for identical ids, so we'll check the properties here too.
    # This is to catch when we're passing the indexed instance of DatasetLite vs the one Loaded from the file.
    # (eg. only the indexed one will have archived information.)
    for i, mismatch in enumerate(sorted_mismatches):
        expected_mismatch = sorted_expected_mismatches[i]

        if not expected_mismatch.dataset:
            assert not mismatch.dataset
        else:
            assert expected_mismatch.dataset.__dict__ == mismatch.dataset.__dict__

    return mismatches


def _check_mismatch_fix(index: MemoryDatasetPathIndex,
                        mismatches: Iterable[Mismatch],
                        expected_index_result: Mapping[DatasetLite, Iterable[str]],
                        fix_settings: dict):
    """Check that the index is correctly updated when fixing mismatches"""

    # First check that no change is made to the index if we have all fixes set to False.
    starting_index = index.as_map()
    # Default settings are all false.
    fixes.fix_mismatches(mismatches, index)
    assert starting_index == index.as_map(), "Changes made to index despite all fix settings being " \
                                             "false (index_missing=False etc)"

    # Now perform fixes, check that they match expected.
    fixes.fix_mismatches(mismatches, index, **fix_settings)
    assert expected_index_result == index.as_map()
