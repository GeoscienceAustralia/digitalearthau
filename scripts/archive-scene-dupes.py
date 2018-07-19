"""
Find duplicate scenes by grouping by [path, row, day]

Chooses the one with children, or else the newest,
and archives the others.
"""
from uuid import UUID

import structlog
from typing import List, Tuple, Set, Iterable

from datacube import Datacube
from datacube.index._api import Index
from datacube.model import Dataset
from digitalearthau.uiutil import init_logging

_LOG = structlog.getLogger()


def main(dry_run=True):
    init_logging()
    index = Datacube().index

    for group_name, dataset_ids in _find_dupes(index):
        log = _LOG.bind(duplicate_group=group_name)
        log.debug("duplicate.found", dataset_ids=dataset_ids)

        to_remove = _choose_removable(dataset_ids, index, log)

        if to_remove:
            removable_ids = [dataset.id for dataset in to_remove]
            log.info('duplicate.archive', dataset_ids=removable_ids)
            if not dry_run:
                index.datasets.archive(removable_ids)
            for dataset in to_remove:
                log.info('duplicate.archive_location', dataset_id=dataset.id, uri=dataset.local_uri)
                if not dry_run:
                    index.datasets.archive_location(dataset.id, dataset.local_uri)


#pylint: disable-msg=consider-using-set-comprehension
def _choose_removable(dataset_ids: List[UUID], index: Index, log) -> List[Dataset]:
    # for each id: does one of them have active children?
    with_children, without_children = _group_by_has_children(dataset_ids, index)

    # If only one has children, it's the one to keep.
    if len(with_children) == 1:
        return [index.datasets.get(dataset_id) for dataset_id in without_children]

    # If multiple have children, warn and skip for another day.
    # We haven't seen this case yet.
    if len(with_children) > 1:
        log.warn('duplicate.too_many_have_children', dataset_ids=with_children, child_count=len(with_children))
        return []

    # Otherwise none have children. Keep the newest.

    datasets = [index.datasets.get(dataset_id) for dataset_id in dataset_ids]

    # Duplicated scenes should have the same label, right?
    # (level 1 could differ in processing level: P41 etc)
    dataset_labels = set([dataset.metadata_doc['ga_label'] for dataset in datasets])
    if len(dataset_labels) != 1:
        log.warn('duplicates.differ', labels=dataset_labels)
        return []

    # Keep newest, archive others.
    datasets.sort(key=lambda d: d.metadata_doc['creation_dt'], reverse=True)

    to_keep = datasets[0]
    to_remove = datasets[1:]

    log.debug("duplicate.choosing_newest",
              keep=to_keep.metadata_doc['creation_dt'],
              remove=[d.metadata_doc['creation_dt'] for d in to_remove])

    if not to_keep.local_path.exists():
        log.warn('newest.not_exist', newest_path=to_keep.local_path)
        return []

    return to_remove
#pylint: enable-msg=consider-using-set-comprehension


def _group_by_has_children(ids: Iterable[UUID], index: Index) -> Tuple[Set[UUID], Set[UUID]]:
    with_children = set()
    without_children = set()
    for dataset_id in ids:
        derived = index.datasets.get_derived(dataset_id)
        if any(child.is_active for child in derived):
            with_children.add(dataset_id)
        else:
            without_children.add(dataset_id)
    return with_children, without_children


# Need to expand the api to avoid using sql here (better grouped search).
# pylint: disable=protected-access
# noinspection PyProtectedMember
def _find_dupes(index: Index) -> List[Tuple[str, List[UUID]]]:
    with index.datasets._db.begin() as conn:
        rows = list(conn._connection.execute("""
            select
                concat_ws(',', product_name, day, path, row) as group_name,
                dataset_ids
            from (
                select
                     dt.name as product_name,
                     d.metadata->'image'->'satellite_ref_point_start'->'x' as path,
                     d.metadata->'image'->'satellite_ref_point_start'->'y' as row,
                     date(
                        ((d.metadata->'extent'->>'center_dt')::timestamp at time zone 'UTC')
                        at time zone 'AEST' ) as day,
                     count(*) as count,
                     array_agg(d.id) as dataset_ids
                from agdc.dataset d
                  inner join agdc.dataset_type dt on dt.id = dataset_type_ref
                where dt.name like '%%_scene'
                and d.archived is null
                group by day, path, row, product_name
            ) scene_pathrows
            where count > 1
        """))
    return rows


if __name__ == '__main__':
    main(dry_run=True)
