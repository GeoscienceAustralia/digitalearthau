import collections
import uuid
from typing import Iterable, List, Mapping, Tuple, Optional

import pytest
import structlog

from datacubenci import sync
from datacubenci.archive import CleanConsoleRenderer
from datacubenci.paths import write_files
from datacubenci.sync import DatasetLite


# These are ok in tests.
# pylint: disable=too-many-locals, protected-access

class MemoryDatasetPathIndex(sync.DatasetPathIndex):
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

    def as_map(self) -> Mapping[DatasetLite, Tuple[str]]:
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
    _check_sync(
        path_search_root=root.joinpath('ls8_scenes'),
        expected_paths=[
            missing_uri,
            on_disk_uri
        ],
        expected_mismatches=[
            sync.LocationMissingOnDisk(old_indexed, missing_uri),
            sync.DatasetNotIndexed(on_disk, on_disk_uri)
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
        path_search_root=root.joinpath('ls8_scenes'),
        expected_paths=[
            on_disk_uri
        ],
        expected_mismatches=[
            sync.LocationMissingOnDisk(old_indexed, on_disk_uri),
            sync.DatasetNotIndexed(on_disk, on_disk_uri),
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
        path_search_root=root.joinpath('ls8_scenes'),
        expected_paths=[
            on_disk_uri,
            missing_uri
        ],
        expected_mismatches=[
            sync.LocationMissingOnDisk(old_indexed, on_disk_uri),
            sync.LocationNotIndexed(on_disk, on_disk_uri),
            sync.LocationMissingOnDisk(on_disk, missing_uri),
        ],
        expected_index_result={
            on_disk: (on_disk_uri,),
            old_indexed: ()
        },
        index=index,
        cache_path=root
    )


def _check_sync(expected_paths, index, path_search_root,
                expected_mismatches, expected_index_result: Mapping[DatasetLite, Tuple[str]], cache_path):
    log = structlog.getLogger()

    cache_path = cache_path.joinpath(str(uuid.uuid4()))
    cache_path.mkdir()

    _check_pathset_loading(cache_path, expected_paths, index, log, path_search_root)
    mismatches = _check_mismatch_find(cache_path, expected_mismatches, index, log, path_search_root)

    # Apply function should result in the expected index.
    sync.fix_index_mismatches(log, index, mismatches)
    assert expected_index_result == index.as_map()


def _check_mismatch_find(cache_path, expected_mismatches, index, log, path_search_root):
    # Now check the actual mismatch output
    mismatches = []
    for mismatch in sync.find_index_disk_mismatches(log, index, path_search_root, cache_path=cache_path):
        print(repr(mismatch))
        mismatches.append(mismatch)
    assert set(mismatches) == set(expected_mismatches)

    return mismatches


# noinspection PyProtectedMember
def _check_pathset_loading(cache_path, expected_paths, index, log, path_search_root):
    path_set = sync._build_pathset(log, path_search_root, index, cache_path)
    # All the paths we expect should be there.
    for expected_path in expected_paths:
        assert expected_path in path_set
    # Nothing else: length matches. There's no len() on the dawg.
    loaded_path_count = sum(1 for p in path_set.iterkeys('file://'))
    assert loaded_path_count == len(expected_paths)
    # Sanity check that a random path doesn't match...
    dummy_dataset = cache_path.joinpath('dummy_dataset', 'ga-metadata.yaml')
    assert dummy_dataset.absolute().as_uri() not in path_set
