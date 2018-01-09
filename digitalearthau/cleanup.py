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
from digitalearthau import paths, uiutil

TRASH_OPTIONS = ui.compose(
    click.option('--min-trash-age-hours',
                 type=int,
                 default=72,
                 help="Only trash locations that were archive at least this many hours ago."),
    click.option('--dry-run',
                 is_flag=True,
                 help="Don't make any changes (ie. don't trash anything)"),
)


@click.group(help='Find and trash archived locations.')
@ui.global_cli_options
def main():
    pass


@main.command('archived')
@TRASH_OPTIONS
@ui.pass_index()
@click.argument('files',
                type=click.Path(exists=True, readable=True),
                nargs=-1)
def archived(index: Index,
             dry_run: bool,
             files: List[str],
             min_trash_age_hours: int):
    """
    Cleanup locations that have been archived.
    """
    latest_time_to_archive = _as_utc(datetime.utcnow()) - timedelta(hours=min_trash_age_hours)
    total_count = 0
    total_trash_count = 0

    # TODO: Get defined collections for path?
    work_path = paths.get_product_work_directory('all', task_type='clean')
    uiutil.init_logging(work_path.joinpath('log.jsonl').open('a'))
    log = structlog.getLogger("cleanup-archived")

    log.info("cleanup.start", dry_run=dry_run, input_paths=files, min_trash_age_hours=min_trash_age_hours)

    for f in files:
        p = Path(f)

        p_uri = p.absolute().as_uri()
        assert p_uri.startswith('file:')
        p_uri_body = p_uri.split('file:')[1]

        echo(f"Cleaning {style(p_uri, bold=True)}", err=True)
        echo(f"  Output {work_path}", err=True)

        with index.datasets._db.begin() as db:
            locations = [
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
                            pgapi.DATASET_LOCATION.c.uri_body.like(p_uri_body + '%'),
                            pgapi.DATASET_LOCATION.c.archived < latest_time_to_archive
                        )
                    )
                )
            ]

        echo(f"  {len(locations)} locations archived more than {min_trash_age_hours}hr ago", err=True)

        with click.progressbar(locations,
                               # stderr should be used for runtime information, not stdout
                               file=sys.stderr) as location_iter:
            for uri in location_iter:
                total_count += 1
                log = log.bind(uri=uri)
                # Multiple datasets can point to the same location (eg. a stacked file).
                datasets = list(index.datasets.get_datasets_for_location(uri))

                # Check that there's no other active locations for this dataset.
                active_dataset = _get_dataset_where_active(uri, datasets)
                if active_dataset:
                    log.info(f"location.has_active", active_dataset_id=active_dataset.id)
                    continue

                paths.trash_uri(uri, dry_run=dry_run, log=log)
                if not dry_run:
                    for dataset in datasets:
                        index.datasets.remove_location(dataset.id, uri)
                total_trash_count += 1

                log = log.unbind('uri')

    log.info("cleanup.finish", total_count=total_count, trash_count=total_trash_count)
    echo(f"Finished; {total_trash_count} trashed.", err=True)


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
    main()
