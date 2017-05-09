import collections
import logging
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, List, Mapping, Optional

import pytest
import structlog

from datacubenci.archive import CleanConsoleRenderer
from datacubenci.collections import Collection
from datacubenci.paths import write_files
from datacubenci.sync import differences as mm, fixes, scan, Mismatch
from datacubenci.sync.index import DatasetLite, DatasetPathIndex


# These are ok in tests.
# pylint: disable=too-many-locals, protected-access


class MemoryDatasetPathIndex(DatasetPathIndex):
    """
    An in-memory implementation, so that we can test without using a real datacube index.
    """

    def get(self, dataset_id: uuid.UUID) -> Optional[DatasetLite]:
        for d in self._records.keys():
            if d.id == dataset_id:
                return d
        return None

    def __init__(self):
        super().__init__()
        # Map of dataset to locations.
        # type: Mapping[DatasetLite, List[str]]
        self._records = collections.defaultdict(list)

    def iter_all_uris(self) -> Iterable[str]:
        for uris in self._records.values():
            yield from uris

    def add_location(self, dataset: DatasetLite, uri: str) -> bool:
        if dataset not in self._records:
            raise ValueError("Unknown dataset {} -> {}".format(dataset.id, uri))

        return self._add(dataset, uri)

    def _add(self, dataset_id, uri):
        if uri in self._records[dataset_id]:
            # Not added
            return False

        self._records[dataset_id].append(uri)
        return True

    def remove_location(self, dataset: DatasetLite, uri: str) -> bool:

        if uri not in self._records[dataset]:
            # Not removed
            return False
        # We never remove the dataset key, only the uris.
        self._records[dataset].remove(uri)

    def get_datasets_for_uri(self, uri: str) -> Iterable[DatasetLite]:
        for dataset, uris in self._records.items():
            if uri in uris:
                yield dataset

    def as_map(self) -> Mapping[DatasetLite, Iterable[str]]:
        """
        All contained (dataset, [location]) values, to check test results.
        """
        return {id_: tuple(uris) for id_, uris in self._records.items()}

    def add_dataset(self, dataset: DatasetLite, uri: str):
        # We're not actually storing datasets...
        return self._add(dataset, uri)


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


def test_index_disk_sync():
    on_disk = DatasetLite(uuid.UUID('1e47df58-de0f-11e6-93a4-185e0f80a5c0'))
    on_disk2 = DatasetLite(uuid.UUID('3604ee9c-e1e8-11e6-8148-185e0f80a5c0'))

    root = write_files(
        {
            'ls8_scenes': {
                'ls8_test_dataset': {
                    'ga-metadata.yaml':
                        ('id: %s\n' % on_disk.id),
                    'dummy-file.txt': ''
                }
            },
            'ls7_scenes': {
                'ls7_test_dataset': {
                    'ga-metadata.yaml':
                        ('id: %s\n' % on_disk2.id)
                }
            }
        }
    )
    cache_path = root.joinpath('cache')
    cache_path.mkdir()

    on_disk_uri = root.joinpath('ls8_scenes', 'ls8_test_dataset', 'ga-metadata.yaml').as_uri()
    on_disk_uri2 = root.joinpath('ls7_scenes', 'ls7_test_dataset', 'ga-metadata.yaml').as_uri()

    # An indexed file not on disk, and disk file not in index.
    index = MemoryDatasetPathIndex()
    missing_uri = root.joinpath('indexed', 'already', 'ga-metadata.yaml').as_uri()
    old_indexed = DatasetLite(uuid.UUID('b9d77d10-e1c6-11e6-bf63-185e0f80a5c0'))
    index.add_dataset(old_indexed, missing_uri)

    ls8_collection = Collection('ls8_scenes', {}, root.joinpath('ls8_scenes'), 'ls*/ga-metadata.yaml', [])
    _check_sync(
        collection=ls8_collection,
        expected_paths=[
            missing_uri,
            on_disk_uri
        ],
        expected_mismatches=[
            mm.LocationMissingOnDisk(old_indexed, missing_uri),
            mm.DatasetNotIndexed(on_disk, on_disk_uri)
        ],
        expected_index_result={
            on_disk: (on_disk_uri,),
            old_indexed: ()
        },
        index=index,
        cache_path=root
    )

    # File on disk has a different id to the one in the index (ie. it was quietly reprocessed)
    index = MemoryDatasetPathIndex()
    index.add_dataset(old_indexed, on_disk_uri)
    _check_sync(
        collection=ls8_collection,
        expected_paths=[
            on_disk_uri
        ],
        expected_mismatches=[
            mm.LocationMissingOnDisk(old_indexed, on_disk_uri),
            mm.DatasetNotIndexed(on_disk, on_disk_uri),
        ],
        expected_index_result={
            on_disk: (on_disk_uri,),
            old_indexed: ()
        },
        index=index,
        cache_path=root
    )

    # File on disk was moved without updating index, replacing existing indexed file location.
    index = MemoryDatasetPathIndex()
    index.add_dataset(old_indexed, on_disk_uri)
    index.add_dataset(on_disk, missing_uri)
    _check_sync(
        collection=ls8_collection,
        expected_paths=[
            on_disk_uri,
            missing_uri
        ],
        expected_mismatches=[
            mm.LocationMissingOnDisk(old_indexed, on_disk_uri),
            mm.LocationNotIndexed(on_disk, on_disk_uri),
            mm.LocationMissingOnDisk(on_disk, missing_uri),
        ],
        expected_index_result={
            on_disk: (on_disk_uri,),
            old_indexed: ()
        },
        index=index,
        cache_path=root
    )

    # A an already-archived file in on disk
    index = MemoryDatasetPathIndex()
    two_days_ago = datetime.utcnow() - timedelta(days=2)
    archived_on_disk = DatasetLite(on_disk.id, archived_time=two_days_ago)
    index.add_dataset(archived_on_disk, on_disk_uri)
    _check_sync(
        collection=ls8_collection,
        expected_paths=[
            on_disk_uri
        ],
        expected_mismatches=[
            mm.ArchivedDatasetOnDisk(archived_on_disk, on_disk_uri),
        ],
        expected_index_result={
            on_disk: (on_disk_uri,),
        },
        index=index,
        cache_path=root
    )


def _check_sync(expected_paths: Iterable[str],
                index: MemoryDatasetPathIndex,
                collection: Collection,
                expected_mismatches: Iterable[Mismatch],
                expected_index_result: Mapping[DatasetLite, Iterable[str]],
                cache_path: Path):
    """Check the correct outputs come from the given sync inputs"""
    log = structlog.getLogger()

    cache_path = cache_path.joinpath(str(uuid.uuid4()))
    cache_path.mkdir()

    _check_pathset_loading(cache_path, expected_paths, index, log, collection)

    mismatches = _check_mismatch_find(cache_path, expected_mismatches, index, log, collection)

    _check_mismatch_fix(index, mismatches, expected_index_result)


# noinspection PyProtectedMember
def _check_pathset_loading(cache_path: Path,
                           expected_paths: Iterable[str],
                           index: MemoryDatasetPathIndex,
                           log: logging.Logger,
                           collection: Collection):
    """Check that the right mix of paths (index and filesystem) are loaded"""
    path_set = scan._build_pathset(log, collection.base_path, collection.offset_pattern, index, cache_path)

    loaded_paths = set(path_set.iterkeys('file://'))
    assert loaded_paths == set(expected_paths)

    # Sanity check that a random path doesn't match...
    dummy_dataset = cache_path.joinpath('dummy_dataset', 'ga-metadata.yaml')
    assert dummy_dataset.absolute().as_uri() not in path_set


def _check_mismatch_find(cache_path, expected_mismatches, index, log, collection: Collection):
    """Check that the correct mismatches were found"""

    mismatches = []
    for mismatch in scan.find_index_disk_mismatches(log, index, collection.base_path, collection.offset_pattern,
                                                    cache_path=cache_path):
        print(repr(mismatch))
        mismatches.append(mismatch)

    def mismatch_sort_key(m):
        return m.__class__.__name__, m.dataset.id, m.uri

    sorted_mismatches = sorted(mismatches, key=mismatch_sort_key)
    sorted_expected_mismatches = sorted(expected_mismatches, key=mismatch_sort_key)

    assert sorted_mismatches == sorted_expected_mismatches

    # DatasetLite.__eq__ only tests for identical ids, so we'll check the properties here too.
    # This is to catch when we're passing the indexed instance of DatasetLite vs the one Loaded from the file.
    # (eg. only the indexed one will have archived information.)
    for i, mismatch in enumerate(sorted_mismatches):
        expected_mismatch = sorted_expected_mismatches[i]
        assert expected_mismatch.dataset.__dict__ == mismatch.dataset.__dict__

    return mismatches


def _check_mismatch_fix(index: MemoryDatasetPathIndex,
                        mismatches: Iterable[Mismatch],
                        expected_index_result: Mapping[DatasetLite, Iterable[str]]):
    """Check that the index is correctly updated when fixing mismatches"""

    # First check that no change is made to the index if we have all fixes set to False.
    starting_index = index.as_map()
    # Default settings are all false.
    fixes.fix_mismatches(mismatches, index)
    assert starting_index == index.as_map(), "Changes made to index despite all fix settings being " \
                                             "false (index_missing=False etc)"

    # Now perform fixes, check that they match expected.
    fixes.fix_mismatches(mismatches, index, index_missing=True, update_locations=True)
    assert expected_index_result == index.as_map()
