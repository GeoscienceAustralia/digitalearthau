# coding=utf-8
"""
Sync a datacube index to a folder of datasets on disk.

Locations will be added/removed according to whether they're on disk, extra datasets will be indexed, etc.
"""
import dawg
import logging
import sys
import uuid
from collections import namedtuple
from datetime import datetime
from itertools import chain
from pathlib import Path
from typing import Iterable, Any, Mapping, Optional
from typing import List
from uuid import UUID

import click
import structlog
from boltons import fileutils
from boltons import strutils
from datacubenci import paths
from datacubenci.archive import CleanConsoleRenderer
from datacubenci.collections import NCI_COLLECTIONS

from datacube.index import index_connect
from datacube.index._api import Index
from datacube.model import Dataset
from datacube.scripts import dataset as dataset_script
from datacube.ui import click as ui
from datacube.utils import uri_to_local_path

_LOG = structlog.get_logger()


class DatasetLite:
    """
    A small subset of datacube.model.Dataset.

    A "real" dataset needs a lot of initialisation: types etc, so this is easier to test with.

    We also, in this script, depend heavily on the __eq__ behaviour of this particular class (by id only), and subtle
    bugs could occur if the core framework made changes to it.
    """

    def __init__(self, id_: uuid.UUID, archived_time: datetime = None):
        # Sanity check of the type, as our equality checks are quietly wrong if the types don't match,
        # and we've previously had problems with libraries accidentally switching string/uuid types...
        assert isinstance(id_, uuid.UUID)
        self.id = id_

        self.archived_time = archived_time

    @property
    def is_archived(self):
        """
        Is this dataset archived?

        (an archived dataset is one that is not intended to be used by users anymore: eg. it has been
        replaced by another dataset. It will not show up in search results, but still exists in the
        system via provenance chains or through id lookup.)

        :rtype: bool
        """
        return self.archived_time is not None

    def __eq__(self, other):
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)

    @classmethod
    def from_agdc(cls, dataset: Dataset):
        return DatasetLite(dataset.id, archived_time=dataset.archived_time)

    def __repr__(self):
        return _simple_object_repr(self)


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

    def get_datasets_for_uri(self, uri: str) -> Iterable[DatasetLite]:
        raise NotImplementedError

    def get(self, dataset_id: uuid.UUID) -> Optional[DatasetLite]:
        raise NotImplementedError

    def add_location(self, dataset: DatasetLite, uri: str) -> bool:
        raise NotImplementedError

    def remove_location(self, dataset: DatasetLite, uri: str) -> bool:
        raise NotImplementedError

    def add_dataset(self, dataset: DatasetLite, uri: str):
        raise NotImplementedError


class AgdcDatasetPathIndex(DatasetPathIndex):
    def __init__(self, index: Index, query: dict):
        super().__init__()
        self._index = index
        self._query = query
        self._rules = dataset_script.load_rules_from_types(self._index)

    def iter_all_uris(self) -> Iterable[str]:
        for uri, in self._index.datasets.search_returning(['uri'], **self._query):
            yield str(uri)

    @classmethod
    def connect(cls, query: Mapping[str, Any]) -> 'AgdcDatasetPathIndex':
        return cls(index_connect(application_name='datacubenci-pathsync'), query=query)

    def get_datasets_for_uri(self, uri: str) -> Iterable[DatasetLite]:
        for d in self._index.datasets.get_datasets_for_location(uri=uri):
            yield DatasetLite.from_agdc(d)

    def remove_location(self, dataset: DatasetLite, uri: str) -> bool:
        was_removed = self._index.datasets.remove_location(dataset.id, uri)
        return was_removed

    def get(self, dataset_id: uuid.UUID) -> Optional[DatasetLite]:
        agdc_dataset = self._index.datasets.get(dataset_id)
        return DatasetLite.from_agdc(agdc_dataset) if agdc_dataset else None

    def add_location(self, dataset: DatasetLite, uri: str) -> bool:
        was_removed = self._index.datasets.add_location(dataset.id, uri)
        return was_removed

    def add_dataset(self, dataset: DatasetLite, uri: str):
        path = uri_to_local_path(uri)

        for d in dataset_script.load_datasets([path], self._rules):
            if d.id == dataset.id:
                self._index.datasets.add(d, sources_policy='ensure')
                break
        else:
            raise RuntimeError('Dataset not found at path: %s, %s' % (dataset.id, uri))

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        self._index.close()


def _build_pathset(
        log: logging.Logger,
        path_search_root: Path,
        path_offset_glob: str,
        path_index: DatasetPathIndex,
        cache_path: Path = None) -> dawg.CompletionDAWG:
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
                (path.absolute().as_uri() for path in path_search_root.glob(path_offset_glob))
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

    def __init__(self, dataset: DatasetLite, uri: str):
        super().__init__()
        self.dataset = dataset
        self.uri = uri

    def fix(self, index: DatasetPathIndex):
        """
        Fix this issue on the given index.
        """
        raise NotImplementedError

    def __repr__(self, *args, **kwargs):
        """
        >>> Mismatch(DatasetLite(UUID('96519c56-e133-11e6-a29f-185e0f80a5c0')), uri='/tmp/test')
        Mismatch(dataset=DatasetLite(archived_time=None, id=UUID('96519c56-e133-11e6-a29f-185e0f80a5c0')), \
uri='/tmp/test')
        """
        return _simple_object_repr(self)

    def __eq__(self, other):
        """
        >>> m = Mismatch(DatasetLite(UUID('96519c56-e133-11e6-a29f-185e0f80a5c0')), uri='/tmp/test')
        >>> m == m
        True
        >>> import copy
        >>> m == copy.copy(m)
        True
        >>> n = Mismatch(DatasetLite(UUID('96519c56-e133-11e6-a29f-185e0f80a5c0')), uri='/tmp/test2')
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

    def fix(self, index: DatasetPathIndex):
        index.remove_location(self.dataset, self.uri)


class LocationNotIndexed(Mismatch):
    """
    An existing dataset has been found at a new location.
    """

    def fix(self, index: DatasetPathIndex):
        index.add_location(self.dataset, self.uri)


class DatasetNotIndexed(Mismatch):
    """
    A dataset has not been indexed.
    """

    def fix(self, index: DatasetPathIndex):
        index.add_dataset(self.dataset, self.uri)


class ArchivedDatasetOnDisk(Mismatch):
    """
    A dataset on disk is already archived in the index.
    """

    def fix(self, index: DatasetPathIndex):
        # We don't fix these yet. It's only here for reporting.
        # TODO: Trash the file if archived more than X days ago?
        pass


def find_index_disk_mismatches(log,
                               path_index: DatasetPathIndex,
                               root_folder: Path,
                               dataset_glob: str,
                               cache_path: Path = None) -> Iterable[Mismatch]:
    """
    Compare the given index and filesystem contents, yielding Mismatches of any differences.
    """
    pathset = _build_pathset(log, root_folder, dataset_glob, path_index, cache_path=cache_path)
    yield from _find_uri_mismatches(pathset.iterkeys('file://'), path_index)


def fix_index_mismatches(log,
                         index: DatasetPathIndex,
                         mismatches: Iterable[Mismatch]):
    for mismatch in mismatches:
        log.debug("mismatch.apply", mismatch=mismatch)
        mismatch.fix(index)


def _find_uri_mismatches(all_file_uris: Iterable[str], index: DatasetPathIndex) -> Iterable[Mismatch]:
    """
    Compare the index and filesystem contents for the given uris,
    yielding Mismatches of any differences.
    """
    for uri in all_file_uris:

        def ids(datasets):
            return [d.id for d in datasets]

        path = uri_to_local_path(uri)
        log = _LOG.bind(path=path)
        log.debug("index.get_dataset_ids_for_uri")
        indexed_datasets = set(index.get_datasets_for_uri(uri))
        datasets_in_file = set(map(DatasetLite, paths.get_path_dataset_ids(path) if path.exists() else []))

        log.info("dataset_ids",
                 indexed_dataset_ids=ids(indexed_datasets),
                 file_ids=ids(datasets_in_file))

        for indexed_dataset in indexed_datasets:
            # Does the dataset exist in the file?
            if indexed_dataset in datasets_in_file:
                if indexed_dataset.is_archived:
                    yield ArchivedDatasetOnDisk(indexed_dataset, uri)
            else:
                yield LocationMissingOnDisk(indexed_dataset, uri)

        # For all file ids not in the index.
        file_ds_not_in_index = datasets_in_file.difference(indexed_datasets)
        log.debug("files_not_in_index", files_not_in_index=file_ds_not_in_index)

        for dataset in file_ds_not_in_index:
            # If it's already indexed, we just need to add the location.
            indexed_dataset = index.get(dataset.id)
            if indexed_dataset:
                yield LocationNotIndexed(indexed_dataset, uri)
            else:
                yield DatasetNotIndexed(dataset, uri)


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
    # type: (Index, List[str], str, bool) -> None

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
                                                       collection.offset_pattern,
                                                       cache_path=collection_cache):
                click.echo('\t'.join(map(str, (
                    collection_name,
                    strutils.camel2under(mismatch.__class__.__name__),
                    mismatch.dataset.id,
                    mismatch.uri
                ))))
                if not dry_run:
                    log.info('mismatch.fix', mismatch=mismatch)
                    mismatch.fix(path_index)


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


def _simple_object_repr(o):
    """
    Calculate a possible repr() for the given object using the class name and all __dict__ properties.

    eg. MyClass(prop1='val1')

    It will call repr() on property values too, so beware of circular dependencies.
    """
    return "%s(%s)" % (
        o.__class__.__name__,
        ", ".join("%s=%r" % (k, v) for k, v in sorted(o.__dict__.items()))
    )


if __name__ == '__main__':
    main()
