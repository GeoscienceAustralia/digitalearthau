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

from datacube.index import Index
from datacube.utils import uri_to_local_path
from digitalearthau import paths
from digitalearthau.collections import Collection
from digitalearthau.index import DatasetLite
from digitalearthau.paths import register_base_directory
from digitalearthau.sync import differences as mm, fixes, scan, Mismatch
from integration_tests.conftest import DatasetForTests, freeze_index


# These are ok in tests. What will linting do?
# pylint: disable=too-many-locals, protected-access, redefined-outer-name


def test_new_and_old_on_disk(test_dataset: DatasetForTests,
                             integration_test_data: Path,
                             other_dataset: DatasetForTests):
    old_indexed = DatasetLite(uuid.UUID('5294efa6-348d-11e7-a079-185e0f80a5c0'))

    # An indexed file not on disk, and disk file not in index.

    missing_dataset = other_dataset

    missing_dataset.add_to_index()

    # Make it missing
    shutil.rmtree(str(missing_dataset.copyable_path))

    _check_sync(
        collection=test_dataset.collection,
        expected_paths=[
            missing_dataset.uri,
            test_dataset.uri
        ],
        expected_mismatches=[
            mm.LocationMissingOnDisk(old_indexed, missing_dataset.uri),
            mm.DatasetNotIndexed(test_dataset.dataset, test_dataset.uri)
        ],
        expected_index_result={
            test_dataset.dataset: (test_dataset.uri,),
            old_indexed: (),
            test_dataset.parent: (),
        },
        cache_path=integration_test_data,
        fix_settings=dict(index_missing=True, update_locations=True)
    )


def test_replace_on_disk(test_dataset: DatasetForTests,
                         integration_test_data: Path,
                         other_dataset: DatasetForTests):
    """
    File on disk has a different id to the one in the index (ie. it was quietly reprocessed)
    """
    test_dataset.add_to_index()

    # move a new one over the top
    shutil.move(other_dataset.path, str(uri_to_local_path(test_dataset.uri)))

    _check_sync(
        collection=test_dataset.collection,
        expected_paths=[
            test_dataset.uri
        ],
        expected_mismatches=[
            mm.LocationMissingOnDisk(test_dataset.dataset, test_dataset.uri),
            mm.DatasetNotIndexed(other_dataset.dataset, test_dataset.uri),
        ],
        expected_index_result={
            test_dataset.dataset: (),
            other_dataset.dataset: (test_dataset.uri,),
            test_dataset.parent: (),
        },
        cache_path=integration_test_data,
        fix_settings=dict(index_missing=True, update_locations=True)
    )


def test_move_on_disk(test_dataset: DatasetForTests,
                      integration_test_data: Path,
                      other_dataset: DatasetForTests):
    """
    Indexed dataset was moved over the top of another indexed dataset
    """
    test_dataset.add_to_index()
    other_dataset.add_to_index()

    shutil.move(other_dataset.path, str(uri_to_local_path(test_dataset.uri)))

    _check_sync(
        collection=test_dataset.collection,
        expected_paths=[
            test_dataset.uri,
            other_dataset.path.as_uri(),
        ],
        expected_mismatches=[
            mm.LocationMissingOnDisk(test_dataset.dataset, test_dataset.uri),
            mm.LocationNotIndexed(other_dataset.dataset, test_dataset.uri),
            mm.LocationMissingOnDisk(other_dataset.dataset, other_dataset.path.as_uri()),
        ],
        expected_index_result={
            test_dataset.dataset: (),
            other_dataset.dataset: (test_dataset.uri,),
            test_dataset.parent: (),
        },
        cache_path=integration_test_data,
        fix_settings=dict(index_missing=True, update_locations=True)
    )


def test_archived_on_disk(test_dataset: DatasetForTests,
                          integration_test_data: Path):
    """
    A an already-archived dataset on disk. Should report it, but not touch the file (trash_archived is false)
    """
    # archived_on_disk = DatasetLite(on_disk.id, archived_time=(datetime.utcnow() - timedelta(days=5)))
    test_dataset.add_to_index()
    test_dataset.archive_in_index()
    archived_time = test_dataset.get_index_record().archived_time

    assert uri_to_local_path(test_dataset.uri).exists(), "On-disk location should exist before test begins."
    _check_sync(
        collection=test_dataset.collection,
        expected_paths=[
            test_dataset.uri
        ],
        expected_mismatches=[
            mm.ArchivedDatasetOnDisk(DatasetLite(test_dataset.dataset.id, archived_time), test_dataset.uri),
        ],
        expected_index_result={
            # Not active in index, as it's archived.
            # on_disk: (on_disk_uri,),
            # But the parent dataset still is:
            test_dataset.parent: (),
        },
        cache_path=integration_test_data,
        fix_settings=dict(index_missing=True, update_locations=True)
    )
    assert uri_to_local_path(test_dataset.uri).exists(), "On-disk location shouldn't be touched"


def test_detect_corrupt_existing(test_dataset: DatasetForTests,
                                 integration_test_data: Path):
    """If a dataset exists but cannot be read, report as corrupt"""
    path = uri_to_local_path(test_dataset.uri)

    test_dataset.add_to_index()
    assert path.exists()

    # Overwrite with corrupted file.
    os.unlink(str(path))
    with path.open('w', encoding='utf8') as f:
        f.write('corruption!')
    assert path.exists()

    # Another dataset exists in the same location

    _check_sync(
        collection=test_dataset.collection,
        expected_paths=[test_dataset.uri],
        expected_mismatches=[
            # We don't know if it's the same dataset
            mm.UnreadableDataset(None, test_dataset.uri)
        ],
        # Unmodified index
        expected_index_result=freeze_index(test_dataset.collection.index_),
        cache_path=integration_test_data,
        fix_settings=dict(trash_missing=True, trash_archived=True, update_locations=True)
    )
    # If a dataset is in the index pointing to the corrupt location, it shouldn't be trashed with trash_archived=True
    assert path.exists(), "Corrupt dataset with sibling in index should not be trashed"


def test_detect_corrupt_new(test_dataset: DatasetForTests,
                            integration_test_data: Path):
    """If a dataset exists but cannot be read handle as corrupt"""

    path = uri_to_local_path(test_dataset.uri)

    # Write corrupted file.
    os.unlink(str(path))
    with path.open('w', encoding='utf8') as f:
        f.write('corruption!')
    assert path.exists()

    # No dataset in index at the corrupt location, so it should be trashed.
    _check_sync(
        collection=test_dataset.collection,
        expected_paths=[test_dataset.uri],
        expected_mismatches=[
            mm.UnreadableDataset(None, test_dataset.uri)
        ],
        expected_index_result={},
        cache_path=integration_test_data,
        fix_settings=dict(trash_missing=True, trash_archived=True, update_locations=True)
    )
    assert not path.exists(), "Corrupt dataset without sibling should be trashed with trash_archived=True"


_TRASH_PREFIX = ('.trash', (datetime.utcnow().strftime('%Y-%m-%d')))


# noinspection PyShadowingNames
def test_remove_missing(test_dataset: DatasetForTests,
                        integration_test_data: Path,
                        other_dataset: DatasetForTests):
    """An on-disk dataset that's not indexed should be trashed when trash_missing=True"""
    register_base_directory(integration_test_data)
    trashed_path = test_dataset.base_path.joinpath(*_TRASH_PREFIX, *test_dataset.path_offset)

    # Add a second dataset that's indexed. Should not be touched!
    other_dataset.add_to_index()

    assert other_dataset.path.exists()

    assert test_dataset.path.exists(), "On-disk location should exist before test begins."
    assert not trashed_path.exists(), "Trashed file shouldn't exit."
    _check_sync(
        collection=test_dataset.collection,
        expected_paths=[
            test_dataset.uri,
            other_dataset.path.as_uri(),
        ],
        expected_mismatches=[
            mm.DatasetNotIndexed(test_dataset.dataset, test_dataset.uri)
        ],
        # Unmodified index
        expected_index_result=freeze_index(test_dataset.collection.index_),
        cache_path=integration_test_data,
        fix_settings=dict(trash_missing=True, update_locations=True)
    )
    assert not test_dataset.path.exists(), "On-disk location should exist before test begins."
    assert trashed_path.exists(), "Trashed file shouldn't exit."
    assert other_dataset.path.exists(), "Dataset outside of collection folder shouldn't be touched"


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
def test_is_trashed(test_dataset: DatasetForTests,
                    integration_test_data: Path,
                    archived_dt,
                    expect_to_be_trashed):
    root = integration_test_data

    # Same test, but trash_archived=True, so it should be renamed to the.
    register_base_directory(root)
    test_dataset.add_to_index()
    test_dataset.archive_in_index(archived_dt=archived_dt)

    archived_on_disk = DatasetLite(test_dataset.dataset.id, archived_time=archived_dt)

    trashed_path = test_dataset.base_path.joinpath(*_TRASH_PREFIX, *test_dataset.path_offset)

    # Before the test, file is in place and nothing trashed.
    assert test_dataset.path.exists(), "On-disk location should exist before test begins."
    assert not trashed_path.exists(), "Trashed file shouldn't exit."
    _check_sync(
        collection=test_dataset.collection,
        expected_paths=[
            test_dataset.uri
        ],
        expected_mismatches=[
            mm.ArchivedDatasetOnDisk(archived_on_disk, test_dataset.uri),
        ],
        expected_index_result={
            # Archived: shouldn't be active in index.
            # on_disk: (on_disk_uri,),
            # Prov parent should still exist as it wasn't archived.
            test_dataset.parent: (),
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
        assert not test_dataset.path.exists(), "On-disk location still exists (should have been moved to trash)."
    else:
        assert not trashed_path.exists(), "File shouldn't have been trashed."
        assert test_dataset.path.exists(), "On-disk location should still be in place."


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

    mismatches = _check_mismatch_find(cache_path, expected_mismatches, collection)

    _check_mismatch_fix(collection.index_, mismatches, expected_index_result, fix_settings=fix_settings)


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


def _check_mismatch_find(cache_path, expected_mismatches, collection: Collection):
    """Check that the correct mismatches were found"""

    mismatches = []

    for mismatch in scan.mismatches_for_collection(collection, cache_path):
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


def _check_mismatch_fix(index: Index,
                        mismatches: Iterable[Mismatch],
                        expected_index_result: Mapping[DatasetLite, Iterable[str]],
                        fix_settings: dict):
    """Check that the index is correctly updated when fixing mismatches"""

    # First check that no change is made to the index if we have all fixes set to False.
    starting_index = freeze_index(index)
    # Default settings are all false.
    fixes.fix_mismatches(mismatches, index)
    assert starting_index == freeze_index(index), "Changes made to index despite all fix settings being " \
                                                  "false (index_missing=False etc)"

    # Now perform fixes, check that they match expected.
    fixes.fix_mismatches(mismatches, index, **fix_settings)
    assert expected_index_result == freeze_index(index)
