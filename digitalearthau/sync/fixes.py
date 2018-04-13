from datetime import datetime, timedelta
from functools import singledispatch
from typing import Iterable, Callable

import structlog
from dateutil import tz

from datacube.index import Index
from digitalearthau.index import add_dataset, get_datasets_for_uri
from digitalearthau.paths import trash_uri
from digitalearthau.sync.differences import UnreadableDataset
from .differences import DatasetNotIndexed, Mismatch, ArchivedDatasetOnDisk, LocationNotIndexed, LocationMissingOnDisk

_LOG = structlog.get_logger()


# underscore function names are the norm with singledispatch
# pylint: disable=function-redefined


@singledispatch
def do_index_missing(mismatch: Mismatch, index: Index):
    pass


@do_index_missing.register(DatasetNotIndexed)
def _add_missing(mismatch: DatasetNotIndexed, index: Index):
    _LOG.info("index_dataset", mismatch=mismatch)
    add_dataset(index, mismatch.dataset.id, mismatch.uri)


@singledispatch
def do_update_locations(mismatch: Mismatch, index: Index):
    pass


@do_update_locations.register(LocationMissingOnDisk)
def _remove_location(mismatch: LocationMissingOnDisk, index: Index):
    _LOG.info("remove_location", mismatch=mismatch)
    index.datasets.remove_location(mismatch.dataset.id, mismatch.uri)


@do_update_locations.register(LocationNotIndexed)
def _add_location(mismatch: LocationNotIndexed, index: Index):
    _LOG.info("add_location", mismatch=mismatch)
    index.datasets.add_location(mismatch.dataset.id, mismatch.uri)


@singledispatch
def do_trash_archived(mismatch: Mismatch, index: Index, min_age_hours: int):
    pass


def _as_utc(d):
    # UTC is default if not specified
    if d.tzinfo is None:
        return d.replace(tzinfo=tz.tzutc())
    return d.astimezone(tz.tzutc())


@do_trash_archived.register(ArchivedDatasetOnDisk)
def _trash_archived_dataset(mismatch: ArchivedDatasetOnDisk, index: Index, min_age_hours: int):
    latest_archived_time = datetime.utcnow().replace(tzinfo=tz.tzutc()) - timedelta(hours=min_age_hours)

    # all datasets at location must have been archived to trash.
    for dataset in index.datasets.get_datasets_for_location(mismatch.uri):
        # Must be archived
        if dataset.archived_time is None:
            _LOG.warning("do_trash_archived.active_siblings", dataset_id=mismatch.dataset.id)
            return
        # Archived more than min_age_hours ago
        if _as_utc(dataset.archived_time) > latest_archived_time:
            _LOG.info("do_trash_archived.too_young", dataset_id=mismatch.dataset.id)
            return

    trash_uri(mismatch.uri)


@singledispatch
def do_trash_missing(mismatch: Mismatch, index: Index):
    pass


@do_trash_missing.register(DatasetNotIndexed)
# An unreadable dataset that passes the below sibling check should be considered missing from the index.
@do_trash_missing.register(UnreadableDataset)
def _trash_missing_dataset(mismatch: DatasetNotIndexed, index: Index):
    # If any (other) indexed datasets exist at the same location we can't trash it.
    datasets_at_location = list(get_datasets_for_uri(index, mismatch.uri))
    if datasets_at_location:
        _LOG.warning("do_trash_missing.indexed_siblings_exist", uri=mismatch.uri)
        return

    trash_uri(mismatch.uri)


def fix_mismatches(mismatches: Iterable[Mismatch],
                   index: Index,
                   index_missing=False,
                   trash_missing=False,
                   trash_archived=False,
                   min_trash_age_hours=72,
                   update_locations=False,
                   pre_fix: Callable[[Mismatch], None] = None):
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
            do_trash_archived(mismatch, index, min_age_hours=min_trash_age_hours)
