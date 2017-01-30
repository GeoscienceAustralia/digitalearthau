import dawg
import logging
import sys
import uuid
from collections import namedtuple
from itertools import chain
from pathlib import Path
from subprocess import check_call
from typing import Iterable, Any, Mapping

import click
import structlog
from boltons import fileutils
from boltons import strutils

from datacube.index import index_connect
from datacube.index._api import Index
from datacube.ui import click as ui
from datacube.utils import uri_to_local_path
from datacubenci import paths
from datacubenci.archive import CleanConsoleRenderer

_LOG = structlog.get_logger()

Collection = namedtuple('Collection', ('query', 'base_path'))

NCI_COLLECTIONS = {
    'telemetry': Collection({'metadata_type': 'telemetry'}, Path('/g/data/v10/repackaged/rawdata/0')),
    'ls8_level1_scene': Collection({'product': 'ls8_level1_scene'}, Path('/g/data/v10/reprocess/ls8/level1')),
    'ls7_level1_scene': Collection({'product': 'ls7_level1_scene'}, Path('/g/data/v10/reprocess/ls7/level1')),
    'ls5_level1_scene': Collection({'product': 'ls5_level1_scene'}, Path('/g/data/v10/reprocess/ls5/level1')),
}


class DatasetPathIndex:
    """
    An index of datasets and their URIs.

    This is a slightly questionable attempt to make testing/mocking simpler.

    There's two implementations: One in-memory and one that uses a real datacube.
    (MemoryDatasetPathIndex and AgdcDatasetPathIndex)
    """

    def __init__(self):
        super().__init__()

    def iter_all_uris(self) -> Iterable[str]:
        raise NotImplementedError

    def get_dataset_ids_for_uri(self, uri: str) -> Iterable[uuid.UUID]:
        raise NotImplementedError

    def has_dataset(self, dataset_id: uuid.UUID) -> bool:
        raise NotImplementedError

    def add_location(self, dataset_id: uuid.UUID, uri: str) -> bool:
        raise NotImplementedError

    def remove_location(self, dataset_id: uuid.UUID, uri: str) -> bool:
        raise NotImplementedError

    def add_dataset(self, dataset_id: uuid.UUID, uri: str):
        raise NotImplementedError


class DatasetLite:
    def __init__(self, id_):
        self.id = id_


class AgdcDatasetPathIndex(DatasetPathIndex):
    def __init__(self, index: Index, query: dict):
        super().__init__()
        self._index = index
        self._query = query

    def iter_all_uris(self) -> Iterable[str]:
        for uri, in self._index.datasets.search_returning(['uri'], **self._query):
            yield str(uri)

    @classmethod
    def connect(cls, query: Mapping[str, Any]) -> 'AgdcDatasetPathIndex':
        return cls(index_connect(application_name='datacubenci-pathsync'), query=query)

    def get_dataset_ids_for_uri(self, uri: str) -> Iterable[uuid.UUID]:
        for dataset in self._index.datasets.get_datasets_for_location(uri=uri):
            yield dataset.id

    def remove_location(self, dataset_id: uuid.UUID, uri: str) -> bool:
        was_removed = self._index.datasets.remove_location(DatasetLite(dataset_id), uri)
        return was_removed

    def has_dataset(self, dataset_id: uuid.UUID) -> bool:
        return self._index.datasets.has(dataset_id)

    def add_location(self, dataset_id: uuid.UUID, uri: str) -> bool:
        was_removed = self._index.datasets.add_location(DatasetLite(dataset_id), uri)
        return was_removed

    def add_dataset(self, dataset_id: uuid.UUID, uri: str):
        path = uri_to_local_path(uri)
        # TODO: Separate this indexing logic from the CLI script to be callable from Python.
        check_call(['datacube', 'dataset', 'add', '--auto-match', str(path)])

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        self._index.close()


def _build_pathset(
        log: logging.Logger,
        path_search_root: Path,
        path_index: DatasetPathIndex,
        cache_path: Path = None,
        metadata_glob="ga-metadata.yaml") -> dawg.CompletionDAWG:
    """
    Build a combined set (in dawg form) of all dataset paths in the given index and filesystem.

    Optionally use the given cache directory to cache repeated builds.
    """
    locations_cache = cache_path.joinpath('locations.dawg') if cache_path else None
    if locations_cache and locations_cache.exists():
        path_set = dawg.CompletionDAWG()
        log.debug("paths.trie.cache.load", file=locations_cache)
        path_set.load(str(locations_cache))
    else:
        log.info("paths.trie.build")
        path_set = dawg.CompletionDAWG(
            chain(
                path_index.iter_all_uris(),
                (path.absolute().as_uri() for path in path_search_root.rglob(metadata_glob))
            )
        )
        log.info("paths.trie.done")
        if locations_cache is not None:
            log.debug("paths.trie.cache.create", file=locations_cache)
            with fileutils.atomic_save(str(locations_cache)) as f:
                path_set.write(f)
    return path_set


class Mismatch:
    """
    A mismatch between index and filesystem.

    See the implementations for different types of mismataches.
    """

    def __init__(self, dataset_id, uri):
        super().__init__()
        self.dataset_id = dataset_id
        self.uri = uri

    def update_index(self, index: DatasetPathIndex):
        """
        Fix this issue on the given index.
        """
        raise NotImplementedError

    def __repr__(self, *args, **kwargs):
        """
        >>> Mismatch(dataset_id='96519c56-e133-11e6-a29f-185e0f80a5c0', uri='/tmp/test')
        Mismatch(dataset_id='96519c56-e133-11e6-a29f-185e0f80a5c0', uri='/tmp/test')
        """
        return "%s(%s)" % (
            self.__class__.__name__,
            ", ".join("%s=%r" % (k, v) for k, v in sorted(self.__dict__.items()))
        )

    def __eq__(self, other):
        """
        >>> m = Mismatch(dataset_id='96519c56-e133-11e6-a29f-185e0f80a5c0', uri='/tmp/test')
        >>> m == m
        True
        >>> import copy
        >>> m == copy.copy(m)
        True
        >>> n = Mismatch(dataset_id='96519c56-e133-11e6-a29f-185e0f80a5c0', uri='/tmp/test2')
        >>> m == n
        False
        """
        if not isinstance(other, self.__class__):
            return False

        return self.__dict__ == other.__dict__

    def __hash__(self):
        return hash(tuple(v for k, v in sorted(self.__dict__.items())))


class LocationMissingOnDisk(Mismatch):
    """
    The dataset is no longer at the given location.

    (Note that there may still be a file at the location, but it is not this dataset)
    """

    def update_index(self, index: DatasetPathIndex):
        index.remove_location(self.dataset_id, self.uri)


class LocationNotIndexed(Mismatch):
    """
    An existing dataset has been found at a new location.
    """

    def update_index(self, index: DatasetPathIndex):
        index.add_location(self.dataset_id, self.uri)


class DatasetNotIndexed(Mismatch):
    """
    A dataset has not been indexed.
    """

    def update_index(self, index: DatasetPathIndex):
        index.add_dataset(self.dataset_id, self.uri)


def find_index_disk_mismatches(log,
                               path_index: DatasetPathIndex,
                               filesystem_root: Path,
                               cache_path: Path = None) -> Iterable[Mismatch]:
    """
    Compare the given index and filesystem contents, yielding Mismatches of any differences.
    """
    pathset = _build_pathset(log, filesystem_root, path_index, cache_path=cache_path)
    yield from _find_uri_mismatches(pathset.iterkeys('file://'), path_index)


def fix_index_mismatches(log,
                         index: DatasetPathIndex,
                         mismatches: Iterable[Mismatch]):
    for mismatch in mismatches:
        log.debug("mismatch.apply", mismatch=mismatch)
        mismatch.update_index(index)


def _find_uri_mismatches(all_file_uris: Iterable[str], index: DatasetPathIndex) -> Iterable[Mismatch]:
    """
    Compare the index and filesystem contents for the given uris,
    yielding Mismatches of any differences.
    """
    for uri in all_file_uris:
        path = uri_to_local_path(uri)
        log = _LOG.bind(path=path)
        log.debug("index.get_dataset_ids_for_uri")
        indexed_dataset_ids = set(index.get_dataset_ids_for_uri(uri))
        file_ids = set(paths.get_path_dataset_ids(path)) if path.exists() else set()
        log.info("dataset_ids", indexed_dataset_ids=indexed_dataset_ids, file_ids=file_ids)

        # Sanity check of the types, as our equality checks below are quietly wrong if the types don't match,
        # and we've previously had problems with libraries accidentally switching string/uuid types...
        assert all(isinstance(id_, uuid.UUID) for id_ in indexed_dataset_ids)
        assert all(isinstance(id_, uuid.UUID) for id_ in file_ids)

        # For all indexed ids not in the file
        indexed_not_in_file = indexed_dataset_ids.difference(file_ids)
        log.debug("indexed_not_in_file", indexed_not_in_file=indexed_not_in_file)
        for dataset_id in indexed_not_in_file:
            yield LocationMissingOnDisk(dataset_id, uri)

        # For all file ids not in the index.
        files_not_in_index = file_ids.difference(indexed_dataset_ids)
        log.debug("files_not_in_index", files_not_in_index=files_not_in_index)

        for dataset_id in files_not_in_index:
            if index.has_dataset(dataset_id):
                yield LocationNotIndexed(dataset_id, uri)
            else:
                yield DatasetNotIndexed(dataset_id, uri)


@click.command()
@ui.global_cli_options
@click.option('--dry-run', is_flag=True, default=False)
@click.option('--cache-folder',
              type=click.Path(exists=True, readable=True, writable=True),
              # 'cache' folder in current directory.
              default='cache')
@click.argument('collections',
                type=click.Choice(NCI_COLLECTIONS.keys()),
                nargs=-1)
@ui.pass_index('datacubenci-sync')
def main(index, collections, cache_folder, dry_run):
    # Direct stuctlog into standard logging.
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M.%S"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            # Coloured output if to terminal.
            CleanConsoleRenderer() if sys.stdout.isatty() else structlog.processors.KeyValueRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    cache = Path(cache_folder)

    for collection_name in collections:
        collection = NCI_COLLECTIONS[collection_name]

        log = _LOG.bind(collection=collection_name)
        collection_cache = cache.joinpath(query_name(collection.query))
        fileutils.mkdir_p(str(collection_cache))

        with AgdcDatasetPathIndex(index, collection.query) as path_index:
            for mismatch in find_index_disk_mismatches(log,
                                                       path_index,
                                                       collection.base_path,
                                                       cache_path=collection_cache):
                click.echo('\t'.join(map(str, (
                    collection_name,
                    strutils.camel2under(mismatch.__class__.__name__),
                    mismatch.dataset_id,
                    mismatch.uri
                ))))
                if not dry_run:
                    log.info('mismatch.fix', mismatch=mismatch)
                    mismatch.update_index(index)


def query_name(query: Mapping[str, Any]) -> str:
    """
    Get a string name for the given query args.

    >>> query_name({'product': 'ls8_level1_scene'})
    'product_ls8_level1_scene'
    >>> query_name({'metadata_type': 'telemetry'})
    'metadata_type_telemetry'
    >>> query_name({'a': '1', 'b': 2, 'c': '"3"'})
    'a_1-b_2-c_3'
    """
    return "-".join(
        '{}_{}'.format(k, strutils.slugify(str(v)))
        for k, v in sorted(query.items())
    )


if __name__ == '__main__':
    main()
