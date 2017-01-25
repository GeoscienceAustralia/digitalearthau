import uuid
from typing import Iterable, List, Mapping

import pytest
import structlog
from boltons.dictutils import MultiDict
from datacubenci import sync
from datacubenci.archive import CleanConsoleRenderer
from datacubenci.paths import write_files


# These are ok in tests.
# pylint: disable=too-many-locals, protected-access

class MemoryDatasetPathIndex(sync.DatasetPathIndex):
    """
    An in-memory implementation, so that we can test without using a real datacube index.
    """

    def has_dataset(self, dataset_id: uuid.UUID) -> bool:
        # noinspection PyCompatibility
        return dataset_id in self._records.itervalues(multi=True)

    def __init__(self):
        super().__init__()
        # Map paths to lists of dataset ids.
        self._records = MultiDict()

    def iter_all_uris(self) -> Iterable[str]:
        return self._records.keys()

    def add_location(self, dataset_id: uuid.UUID, uri: str) -> bool:
        ids = self._records.getlist(uri)
        if dataset_id in ids:
            # Not added
            return False

        self._records.add(uri, dataset_id)
        return True

    def remove_location(self, dataset_id: uuid.UUID, uri: str) -> bool:
        if dataset_id not in self.get_dataset_ids_for_uri(uri):
            # Not removed
            return False

        ids = self._records.popall(uri)
        ids.remove(dataset_id)
        self._records.addlist(uri, ids)

    def get_dataset_ids_for_uri(self, uri: str) -> List[uuid.UUID]:
        return list(self._records.getlist(uri))

    def as_map(self) -> Mapping[uuid.UUID, str]:
        """
        All contained (dataset, location) pairs, to check test results.
        """
        return self._records.inverted().todict()

    def add_dataset(self, dataset_id: uuid.UUID, uri: str):
        # We're not actually storing datasets...
        return self.add_location(dataset_id, uri)


@pytest.fixture(scope="session", autouse=True)
def do_something(request):
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


def test_something():
    on_disk_id = uuid.UUID('1e47df58-de0f-11e6-93a4-185e0f80a5c0')
    on_disk_id2 = uuid.UUID('3604ee9c-e1e8-11e6-8148-185e0f80a5c0')

    root = write_files(
        {
            'ls8_scenes': {
                'ls8_test_dataset': {
                    'ga-metadata.yaml':
                        ('id: %s\n' % on_disk_id),
                    'dummy-file.txt': ''
                }
            },
            'ls7_scenes': {
                'ls7_test_dataset': {
                    'ga-metadata.yaml':
                        ('id: %s\n' % on_disk_id2)
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
    old_indexed_id = uuid.UUID('b9d77d10-e1c6-11e6-bf63-185e0f80a5c0')
    index.add_location(old_indexed_id, missing_uri)
    _check_sync(
        path_search_root=root.joinpath('ls8_scenes'),
        expected_paths=[
            missing_uri,
            on_disk_uri
        ],
        expected_mismatches=[
            sync.LocationMissingOnDisk(old_indexed_id, missing_uri),
            sync.DatasetNotIndexed(on_disk_id, on_disk_uri)
        ],
        expected_index_result={
            on_disk_id: [on_disk_uri],
            old_indexed_id: []
        },
        index=index,
        cache_path=root
    )

    # File on disk has a different id to the one in the index (ie. it was quietly reprocessed)
    index = MemoryDatasetPathIndex()
    index.add_location(old_indexed_id, on_disk_uri)
    _check_sync(
        path_search_root=root.joinpath('ls8_scenes'),
        expected_paths=[
            on_disk_uri
        ],
        expected_mismatches=[
            sync.LocationMissingOnDisk(old_indexed_id, on_disk_uri),
            sync.DatasetNotIndexed(on_disk_id, on_disk_uri),
        ],
        expected_index_result={
            on_disk_id: [on_disk_uri],
            old_indexed_id: []
        },
        index=index,
        cache_path=root
    )

    # File on disk was moved without updating index, replacing existing indexed file location.
    index = MemoryDatasetPathIndex()
    index.add_location(old_indexed_id, on_disk_uri)
    index.add_location(on_disk_id, missing_uri)
    _check_sync(
        path_search_root=root.joinpath('ls8_scenes'),
        expected_paths=[
            on_disk_uri,
            missing_uri
        ],
        expected_mismatches=[
            sync.LocationMissingOnDisk(old_indexed_id, on_disk_uri),
            sync.LocationNotIndexed(on_disk_id, on_disk_uri),
            sync.LocationMissingOnDisk(on_disk_id, missing_uri),
        ],
        expected_index_result={
            on_disk_id: [on_disk_uri],
            old_indexed_id: []
        },
        index=index,
        cache_path=root
    )


def _check_sync(expected_paths, index, path_search_root,
                expected_mismatches, expected_index_result: Mapping[uuid.UUID, List[str]], cache_path):
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
