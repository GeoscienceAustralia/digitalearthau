import dawg
import logging
import multiprocessing
import time
from functools import partial
from itertools import chain
from pathlib import Path
from typing import Iterable, Any, Mapping, List

import structlog
from boltons import fileutils
from boltons import strutils

from datacube.utils import uri_to_local_path, InvalidDocException
from datacubenci import paths
from datacubenci.collections import Collection
from datacubenci.index import DatasetPathIndex, DatasetLite
from datacubenci.sync import validate
from datacubenci.sync.differences import UnreadableDataset, InvalidDataset
from .differences import ArchivedDatasetOnDisk, Mismatch, LocationMissingOnDisk, LocationNotIndexed, \
    DatasetNotIndexed

_LOG = structlog.get_logger()

# 12 hours (roughly the same workday)
CACHE_TIMEOUT_SECS = 60 * 60 * 12


def cache_is_too_old(path):
    if not path.exists():
        return True

    oldest_valid_time = time.time() - CACHE_TIMEOUT_SECS
    return path.stat().st_mtime < oldest_valid_time


def build_pathset(
        log: logging.Logger,
        collection: Collection,
        cache_path: Path = None) -> dawg.CompletionDAWG:
    """
    Build a combined set (in dawg form) of all dataset paths in the given index and filesystem.

    Optionally use the given cache directory to cache repeated builds.
    """
    locations_cache = cache_path.joinpath(query_name(collection.query), 'locations.dawg') if cache_path else None
    if locations_cache:
        fileutils.mkdir_p(str(locations_cache.parent))

    if locations_cache and not cache_is_too_old(locations_cache):
        path_set = dawg.CompletionDAWG()
        log.debug("paths.trie.cache.load", file=locations_cache)
        path_set.load(str(locations_cache))
    else:
        log.info("paths.trie.build")
        path_set = dawg.CompletionDAWG(
            chain(
                collection.iter_index_uris(),
                collection.iter_fs_uris()
            )
        )
        log.info("paths.trie.done")
        if locations_cache is not None:
            log.debug("paths.trie.cache.create", file=locations_cache)
            with fileutils.atomic_save(str(locations_cache)) as f:
                path_set.write(f)
    return path_set


# Suppress "Serializing PostgresDb engine" warning. It's triggered due to using index as a multiprocessing argument.
# It's usually warned against to prevent datacube clients hitting the index from every worker, but it's a valid
# use case with this sync tool, where we have a handful of small workers.
# TODO: Push only the connection setup information? Or have a dedicated process for index info.
logging.getLogger('datacube.index.postgres._connections').setLevel(logging.ERROR)


def _find_uri_mismatches(index: DatasetPathIndex, uri: str, validate_data=True) -> Iterable[Mismatch]:
    """
    Compare the index and filesystem contents for the given uris,
    yielding Mismatches of any differences.
    """

    def ids(datasets):
        return [d.id for d in datasets]

    path = uri_to_local_path(uri)
    log = _LOG.bind(path=path)
    log.debug("index.get_dataset_ids_for_uri")
    indexed_datasets = set(index.get_datasets_for_uri(uri))

    datasets_in_file = set()
    if path.exists():
        try:
            datasets_in_file = set(map(DatasetLite, paths.get_path_dataset_ids(path)))
        except InvalidDocException:
            # Should we do something with indexed_datasets here? If there's none, we're more willing to trash.
            yield UnreadableDataset(None, uri)
            log.exception("invalid_path")
            return

        log.info("dataset_ids",
                 indexed_dataset_ids=ids(indexed_datasets),
                 file_ids=ids(datasets_in_file))

        if validate_data:
            validation_success = validate.validate_dataset(path, log=log)
            if not validation_success:
                yield InvalidDataset(None, uri)
                log.exception("invalid_path")
                return

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


def mismatches_for_collection(collection: Collection,
                              cache_folder: Path,
                              path_index: DatasetPathIndex,
                              # Root folder of all file uris.
                              uri_prefix="file:///",
                              workers=2,
                              work_chunksize=30) -> Iterable[Mismatch]:
    """
    Compare the given index and filesystem contents, yielding Mismatches of any differences.
    """
    log = _LOG.bind(collection=collection.name)

    path_dawg = build_pathset(log, collection, cache_folder)

    # Clean up any open connections before we fork.
    path_index.close()

    with multiprocessing.Pool(processes=workers) as pool:
        result = pool.imap_unordered(
            partial(_find_uri_mismatches_eager, path_index),
            path_dawg.iterkeys(uri_prefix),
            chunksize=work_chunksize
        )

        for r in result:
            yield from r


def _find_uri_mismatches_eager(index: DatasetPathIndex, uri: str) -> List[Mismatch]:
    return list(_find_uri_mismatches(index, uri))


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
