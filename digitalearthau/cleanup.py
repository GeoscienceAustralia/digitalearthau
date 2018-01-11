import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

import click
import structlog
from click import echo, style
from sqlalchemy import select, and_

from datacube.index._api import Index
from datacube.index.postgres import _api as pgapi
from datacube.ui import click as ui
from datacube.utils import uri_to_local_path
from digitalearthau import paths, uiutil
from dateutil import tz


@click.group(help='Find and trash archived locations.')
@ui.global_cli_options
def cli():
    pass


@cli.command('archived')
@click.option('--min-trash-age-hours',
              type=int,
              default=72,
              help="Only trash locations that were archive at least this many hours ago.")
@click.option('--dry-run',
              is_flag=True,
              help="Don't make any changes (ie. don't trash anything)")
@ui.pass_index()
@click.argument('files',
                type=click.Path(exists=True, readable=True),
                nargs=-1)
def archived(index: Index,
             dry_run: bool,
             files: List[str],
             min_trash_age_hours: int):
    """
    Cleanup any datasets within the given file path(s)

    It will trash any file with a location archived more than min-trash-age-hours ago.
    """
    total_count = 0
    total_trash_count = 0

    # TODO: Get defined collections for path?
    work_path = paths.get_product_work_directory('all', task_type='clean')
    uiutil.init_logging(work_path.joinpath('log.jsonl').open('a'))
    log = structlog.getLogger("cleanup-archived")

    log.info("cleanup.start", dry_run=dry_run, input_paths=files, min_trash_age_hours=min_trash_age_hours)
    echo(f"Logging to {work_path}", err=True)

    for input_file in files:
        count, trash_count = _cleanup_uri(
            dry_run,
            index,
            Path(input_file).absolute().as_uri(),
            min_trash_age_hours,
            log
        )
        total_count += count
        total_trash_count += trash_count

    log.info("cleanup.finish", total_count=total_count, trash_count=total_trash_count)
    echo(f"Finished; {total_trash_count} trashed.", err=True)


def _cleanup_uri(dry_run: bool,
                 index: Index,
                 input_uri: str,
                 min_trash_age_hours: int,
                 log):
    trash_count = 0

    latest_time_to_archive = _as_utc(datetime.utcnow()) - timedelta(hours=min_trash_age_hours)

    echo(f"Cleaning {'(dry run) ' if dry_run else ''}{style(input_uri, bold=True)}", err=True)

    locations = _get_archived_locations_within(index, latest_time_to_archive, input_uri)
    echo(f"  {len(locations)} locations archived more than {min_trash_age_hours}hr ago", err=True)
    with click.progressbar(locations,
                           # stderr should be used for runtime information, not stdout
                           file=sys.stderr) as location_iter:
        for uri in location_iter:
            log = log.bind(uri=uri)
            local_path = uri_to_local_path(uri)
            if not local_path.exists():
                # An index record exists, but the file isn't on the disk.
                # We won't remove the record from the index: maybe the filesystem is temporarily unmounted?
                log.warning('location.not_exist')
                continue

            # Multiple datasets can point to the same location (eg. a stacked file).
            indexed_datasets = set(index.datasets.get_datasets_for_location(uri))

            # Check that there's no other active locations for this dataset.
            active_dataset = _get_dataset_where_active(uri, indexed_datasets)
            if active_dataset:
                log.info("location.has_active", active_dataset_id=active_dataset.id)
                continue

            # Are there any dataset ids in the file that we haven't indexed? Skip it.
            unindexed_ids = get_unknown_dataset_ids(index, uri)
            if unindexed_ids:
                log.info('location.has_unknown', unknown_dataset_ids=unindexed_ids)
                continue

            was_trashed = paths.trash_uri(uri, dry_run=dry_run, log=log)
            if not dry_run:
                for dataset in indexed_datasets:
                    index.datasets.remove_location(dataset.id, uri)

            if was_trashed:
                trash_count += 1

            log = log.unbind('uri')
    return len(locations), trash_count


def get_unknown_dataset_ids(index, uri):
    """Get ids of datasets in the file that have never been indexed"""
    on_disk_dataset_ids = set(paths.get_path_dataset_ids(uri_to_local_path(uri)))
    unknown_ids = set()
    for dataset_id in on_disk_dataset_ids:
        if not index.datasets.has(dataset_id):
            unknown_ids.add(dataset_id)

    return unknown_ids


# TODO: expand api to support this?
# pylint: disable=protected-access
def _get_archived_locations_within(index, latest_time_to_archive, uri) -> set:
    assert uri.startswith('file:')

    scheme, body = pgapi._split_uri(uri)

    with index.datasets._db.begin() as db:
        locations = set(
            uri for (uri,) in
            db._connection.execute(
                select(
                    [pgapi._dataset_uri_field(pgapi.DATASET_LOCATION)]
                ).select_from(
                    pgapi.DATASET.join(pgapi.DATASET_LOCATION)
                ).where(
                    and_(
                        pgapi.DATASET_LOCATION.c.archived != None,
                        pgapi.DATASET_LOCATION.c.uri_scheme == 'file',
                        pgapi.DATASET_LOCATION.c.uri_body.like(body + '%'),
                        pgapi.DATASET_LOCATION.c.archived < latest_time_to_archive
                    )
                )
            )
        )

    return locations


def _get_dataset_where_active(uri, datasets):
    for d in datasets:
        if uri in d.uris:
            return d
    return None


def _as_utc(d):
    # UTC is default if not specified
    if d.tzinfo is None:
        return d.replace(tzinfo=tz.tzutc())
    return d.astimezone(tz.tzutc())


if __name__ == '__main__':
    cli()
