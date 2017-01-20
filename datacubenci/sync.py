import dawg
import uuid
from itertools import chain
from pathlib import Path
from typing import Iterable, List, Mapping, Tuple

import structlog
from attr import attributes, attrib
from boltons import fileutils
from datacubenci import paths

from datacube.index import index_connect
from datacube.index._api import Index
from datacube.utils import uri_to_local_path

_LOG = structlog.get_logger()


class DatasetPathIndex:
    def __init__(self):
        super().__init__()

    def iter_all_uris(self, product: str) -> Iterable[str]:
        raise NotImplementedError

    def get_dataset_ids_for_uri(self, uri: str) -> List[uuid.UUID]:
        raise NotImplementedError

    def has_dataset(self, dataset_id: uuid.UUID) -> bool:
        raise NotImplementedError

    def add_location(self, dataset_id: uuid.UUID, uri: str) -> bool:
        raise NotImplementedError

    def remove_location(self, dataset_id: uuid.UUID, uri: str) -> bool:
        raise NotImplementedError


class DatasetLite:
    def __init__(self, id):
        self.id = id


class AgdcDatasetPathIndex(DatasetPathIndex):
    def __init__(self, index: Index):
        super().__init__()
        self._index = index

    def iter_all_uris(self, product: str) -> Iterable[str]:
        for uri, in self._index.datasets.search_returning(['uri'], product=product):
            yield str(uri)

    @classmethod
    def connect(cls) -> 'AgdcDatasetPathIndex':
        return cls(index_connect(application_name='datacubenci-pathsync'))

    def get_dataset_ids_for_uri(self, uri: str) -> List[uuid.UUID]:
        return []

    def remove_location(self, dataset_id: uuid.UUID, uri: str) -> bool:
        was_removed = self._index.datasets.remove_location(DatasetLite(dataset_id), uri)
        return was_removed

    def has_dataset(self, dataset_id: uuid.UUID) -> bool:
        pass

    def add_location(self, dataset_id: uuid.UUID, uri: str) -> bool:
        was_removed = self._index.datasets.add_location(DatasetLite(dataset_id), uri)
        return was_removed

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        self._index.close()


def iter_product_pathsets(product_locations: Mapping[str, Path],
                          path_index: DatasetPathIndex,
                          cache_path: Path) -> Iterable[Tuple[str, dawg.CompletionDAWG]]:
    fileutils.mkdir_p(str(cache_path))
    for product, filesystem_root in product_locations.items():
        yield product, _build_pathset(filesystem_root, product, path_index)


def _build_pathset(path_search_root: Path,
                   product: str,
                   path_index: DatasetPathIndex,
                   cache_path: Path = None) -> dawg.CompletionDAWG:
    log = _LOG.bind(product=product)

    locations_cache = cache_path.joinpath(product + '-locations.dawg') if cache_path else None
    if locations_cache and locations_cache.exists():
        path_set = dawg.CompletionDAWG()
        log.debug("paths.trie.cache.load", file=locations_cache)
        path_set.load(str(locations_cache))
    else:
        log.info("paths.trie.build")
        path_set = dawg.CompletionDAWG(
            chain(
                path_index.iter_all_uris(product),
                (path.absolute().as_uri() for path in path_search_root.rglob("ga-metadata.yaml"))
            )
        )
        log.info("paths.trie.done")
        if locations_cache is not None:
            log.debug("paths.trie.cache.create", file=locations_cache)
            with fileutils.atomic_save(str(locations_cache)) as f:
                path_set.write(f)
    return path_set


class Mismatch:
    def __init__(self, dataset_id, uri):
        super().__init__()
        self.dataset_id = dataset_id
        self.uri = uri


class MissingFile(Mismatch):
    pass


class LocationNotIndexed(Mismatch):
    pass


class DatasetNotIndexed(Mismatch):
    pass


def compare_index_and_files(all_file_uris: Iterable[str], index: DatasetPathIndex):
    for uri in all_file_uris:
        path = uri_to_local_path(uri)
        log = _LOG.bind(path=path)

        indexed_dataset_ids = index.get_dataset_ids_for_uri(uri)
        file_ids = paths.get_path_dataset_ids(path) if path.exists() else []
        log.info("dataset_ids", indexed_dataset_ids=indexed_dataset_ids, file_ids=file_ids)

        # Are we making the index look like the filesystem, or the filesystem like the index?

        # For all indexed ids not in the file
        #       # yield MissingFile

        # For all file ids not in the index.
        # - Is the dataset in the index?
        #   - If so:
        #       # yield LocationNotIndexed
        #   - Otherwise
        #       # yield DatasetNotIndexed


def main():
    root = Path('/tmp/test')
    cache = root.joinpath('cache')
    products = {
        'ls8_level1_scene': root.joinpath('ls8_scenes'),
        'ls7_level1_scene': root.joinpath('ls7_scenes'),
    }

    with AgdcDatasetPathIndex.connect() as path_index:
        for product, pathset in iter_product_pathsets(products, path_index, cache_path=cache):
            compare_index_and_files(
                pathset.iterkeys('file://'),
                path_index
            )


if __name__ == '__main__':
    main()
