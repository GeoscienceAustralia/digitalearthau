from functools import singledispatch
from typing import Iterable, Callable

import structlog

from .differences import DatasetNotIndexed, Mismatch, ArchivedDatasetOnDisk, LocationNotIndexed, LocationMissingOnDisk
from .index import DatasetPathIndex

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


@do_trash_archived.register(ArchivedDatasetOnDisk)
def _(mismatch: ArchivedDatasetOnDisk, index: DatasetPathIndex, min_age_hours: int):
    # TODO: Trash if older than X
    pass


@singledispatch
def do_trash_missing(mismatch: Mismatch, index: DatasetPathIndex):
    pass


@do_trash_missing.register(DatasetNotIndexed)
def _(mismatch: DatasetNotIndexed, index: DatasetPathIndex):
    # TODO: Trash
    pass


def fix_mismatches(mismatches: Iterable[Mismatch], index: DatasetPathIndex,
                   index_missing=False, trash_missing=False,
                   trash_archived=False, min_trash_age_hours=72,
                   update_locations=False, pre_fix: Callable[[Mismatch], None]=None):
    for mismatch in mismatches:
        _LOG.info('mismatch.found', mismatch=mismatch)

        if pre_fix:
            pre_fix(mismatch)

        if update_locations:
            do_update_locations(mismatch, index)

        if index_missing:
            do_index_missing(mismatch, index)
        elif trash_missing:
            do_trash_missing(mismatch)

        if trash_archived:
            do_trash_archived(mismatch, index, min_age_hours=min_trash_age_hours)
