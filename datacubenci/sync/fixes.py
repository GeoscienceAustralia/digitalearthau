import os
from datetime import datetime, timedelta
from functools import singledispatch
from typing import Iterable, Callable

import structlog
from dateutil import tz

from datacube.utils import uri_to_local_path
from datacubenci import paths
from datacubenci.index import DatasetPathIndex
from datacubenci.sync.differences import UnreadableDataset
from .differences import DatasetNotIndexed, Mismatch, ArchivedDatasetOnDisk, LocationNotIndexed, LocationMissingOnDisk

_LOG = structlog.get_logger()


# underscore function names are the norm with singledispatch
# pylint: disable=function-redefined

@singledispatch
def do_index_missing(mismatch: Mismatch, index: DatasetPathIndex):
    pass


@do_index_missing.register(DatasetNotIndexed)
def _(mismatch: DatasetNotIndexed, index: DatasetPathIndex):
    index.add_dataset(mismatch.dataset, mismatch.uri)


@singledispatch
def do_update_locations(mismatch: Mismatch, index: DatasetPathIndex):
    pass


@do_update_locations.register(LocationMissingOnDisk)
def _(mismatch: LocationMissingOnDisk, index: DatasetPathIndex):
    index.remove_location(mismatch.dataset, mismatch.uri)


@do_update_locations.register(LocationNotIndexed)
def _(mismatch: LocationNotIndexed, index: DatasetPathIndex):
    index.add_location(mismatch.dataset, mismatch.uri)


@singledispatch
def do_trash_archived(mismatch: Mismatch, index: DatasetPathIndex, min_age_hours: int):
    pass


def _as_utc(d):
    # UTC is default if not specified
    if d.tzinfo is None:
        return d.replace(tzinfo=tz.tzutc())
    return d.astimezone(tz.tzutc())


@do_trash_archived.register(ArchivedDatasetOnDisk)
def _(mismatch: ArchivedDatasetOnDisk, index: DatasetPathIndex, min_age_hours: int):
    latest_archived_time = datetime.utcnow().replace(tzinfo=tz.tzutc()) - timedelta(hours=min_age_hours)

    # all datasets at location must have been archived to trash.
    for dataset in index.get_datasets_for_uri(mismatch.uri):
        # Must be archived
        if dataset.archived_time is None:
            _LOG.warning("do_trash_archived.active_siblings", dataset_id=mismatch.dataset.id)
            return
        # Archived more than min_age_hours ago
        if _as_utc(dataset.archived_time) > latest_archived_time:
            _LOG.info("do_trash_archived.too_young", dataset_id=mismatch.dataset.id)
            return

    _trash(mismatch, index)


@singledispatch
def do_trash_missing(mismatch: Mismatch, index: DatasetPathIndex):
    pass


@do_trash_missing.register(DatasetNotIndexed)
# An unreadable dataset that passes the below sibling check should be considered missing from the index.
@do_trash_missing.register(UnreadableDataset)
def _(mismatch: DatasetNotIndexed, index: DatasetPathIndex):
    # If any (other) indexed datasets exist at the same location we can't trash it.
    datasets_at_location = list(index.get_datasets_for_uri(mismatch.uri))
    if datasets_at_location:
        _LOG.warning("do_trash_missing.indexed_siblings_exist", uri=mismatch.uri)
        return

    _trash(mismatch, index)


def _trash(mismatch: Mismatch, index: DatasetPathIndex):
    local_path = uri_to_local_path(mismatch.uri)

    if not local_path.exists():
        _LOG.warning("trash.not_exist", path=local_path)
        return

    # TODO: to handle sibling-metadata we should trash "all_dataset_paths" too.
    base_path, all_dataset_files = paths.get_dataset_paths(local_path)

    trash_path = paths.get_trash_path(base_path)

    _LOG.info("trashing", base_path=base_path, trash_path=trash_path)
    if not trash_path.parent.exists():
        os.makedirs(str(trash_path.parent))
    os.rename(str(base_path), str(trash_path))


def fix_mismatches(mismatches: Iterable[Mismatch],
                   index: DatasetPathIndex,
                   index_missing=False,
                   trash_missing=False,
                   trash_archived=False,
                   min_trash_age_hours=72,
                   update_locations=False,
                   pre_fix: Callable[[Mismatch], None]=None):
    if index_missing and trash_missing:
        raise RuntimeError("Datasets missing from the index can either be indexed or trashed, but not both.")

    for mismatch in mismatches:
        _LOG.info('mismatch.found', mismatch=mismatch)

        if pre_fix:
            pre_fix(mismatch)

        if update_locations:
            do_update_locations(mismatch, index)

        if index_missing:
            do_index_missing(mismatch, index)
        elif trash_missing:
            do_trash_missing(mismatch, index)

        if trash_archived:
            _LOG.info('mismatch.trash', mismatch=mismatch)
            do_trash_archived(mismatch, index, min_age_hours=min_trash_age_hours)
