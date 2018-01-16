"""
Find duplicate scenes by grouping by [path, row, day]

Chooses the one with children, or else the newest,
and archives the others.
"""
import structlog

from datacube import Datacube
from digitalearthau.uiutil import init_logging

_LOG = structlog.getLogger()


def main(dry_run=True):
    init_logging()
    index = Datacube().index

    with index.datasets._db.begin() as conn:
        rows = list(conn._connection.execute("""
            select * from (
                select
                     dt.name as product_name,
                     d.metadata->'image'->'satellite_ref_point_start'->'x' as path,
                     d.metadata->'image'->'satellite_ref_point_start'->'y' as row,
                     date(
                        ((d.metadata->'extent'->>'center_dt')::timestamp at time zone 'UTC') 
                        at time zone 'AEST' ) as day,
                     count(*) as count,
                     array_agg(d.id) as ids
                from agdc.dataset d
                  inner join agdc.dataset_type dt on dt.id = dataset_type_ref
                where dt.name like '%%_scene'
                and d.archived is null
                group by day, path, row, product_name
            ) scene_pathrows
            where count > 1
        """))

    for product_name, sat_path, sat_row, day, count, ids in rows:
        log = _LOG.bind(product=product_name, sat_path=sat_path, sat_row=sat_row, day=day)
        log.debug("duplicate", count=count, dataset_ids=ids)

        # for each id: does one of them have active children?
        with_children = set()
        without_children = set()

        for dataset_id in ids:
            derived = index.datasets.get_derived(dataset_id)
            if any(child.is_active for child in derived):
                with_children.add(dataset_id)
            else:
                without_children.add(dataset_id)

        # If only one has children, it's the one to keep.
        if len(with_children) == 1:
            to_remove = [index.datasets.get(dataset_id) for dataset_id in without_children]
        elif len(with_children) > 1:
            # Do we have any like this? For now just a warning.
            log.warn('duplicate.too_many_have_children', dataset_ids=with_children, child_count=len(with_children))
            continue
        else:
            # None have children.

            datasets = [index.datasets.get(dataset_id) for dataset_id in ids]

            # Duplicated scenes should have the same label, right?
            dataset_labels = set([dataset.metadata_doc['ga_label'] for dataset in datasets])
            if len(dataset_labels) != 1:
                log.warn('duplicates.differ', labels=dataset_labels)
                continue

            # Keep newest, archive others.
            datasets.sort(key=lambda d: d.metadata_doc['creation_dt'], reverse=True)
            to_remove = datasets[1:]
            log.debug("duplicate.choosing_newest",
                      keep=datasets[0].metadata_doc['creation_dt'],
                      remove=[d.metadata_doc['creation_dt'] for d in to_remove])

            if not datasets[0].local_path.exists():
                log.warn('newest.not_exist', newest_path=datasets[0].local_path)
                continue

        removable_ids = [dataset.id for dataset in to_remove]
        log.info('duplicate.archive', dataset_ids=removable_ids)
        if not dry_run:
            index.datasets.archive(removable_ids)

        for dataset in to_remove:
            log.info('duplicate.archive_location', dataset_id=dataset.id, uri=dataset.local_uri)
            if not dry_run:
                index.datasets.archive_location(dataset.id, dataset.local_uri)


if __name__ == '__main__':
    main(dry_run=True)
