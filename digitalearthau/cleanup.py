"""
Find and trash archived locations of active datasets.
"""
from datetime import datetime, timedelta

import click
import structlog
from dateutil import tz

from datacube.ui import click as ui
from digitalearthau import paths, uiutil

_LOG = structlog.getLogger('cleanup-archived')

trash_options = ui.compose(
    click.option('--only-redundant/--all',
                 is_flag=True,
                 default=True,
                 help='Only trash locations with a second active location'),
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


@main.command('indexed', help="Search the index for archived locations")
@ui.pass_index()
@ui.parsed_search_expressions
@trash_options
def indexed(index, expressions, dry_run, only_redundant, min_trash_age_hours):
    _LOG.info('query', query=expressions)
    datasets = index.datasets.search(**expressions)
    _do_trash(datasets, index, dry_run, only_redundant, min_trash_age_hours)


def _do_trash(datasets, index, dry_run, only_redundant, min_trash_age_hours):
    uiutil.init_logging()

    latest_time_to_archive = _as_utc(datetime.utcnow()) - timedelta(hours=min_trash_age_hours)

    count = 0
    trash_count = 0
    for dataset in datasets:
        count += 1

        log = _LOG.bind(dataset_id=str(dataset.id))

        archived_uri_times = index.datasets.get_archived_location_times(dataset.id)
        if not archived_uri_times:
            log.debug('dataset.nothing_archived')
            continue

        if only_redundant:
            if dataset.uris is not None and len(dataset.uris) == 0:
                # This active dataset has no active locations to replace the one's we're archiving.
                # Probably a mistake? Don't trash the archived ones yet.
                log.warning("dataset.noactive", archived_paths=archived_uri_times)
                continue

        for uri, archived_time in archived_uri_times:
            if _as_utc(archived_time) > latest_time_to_archive:
                log.info('dataset.too_recent')
                continue

            paths.trash_uri(uri, dry_run=dry_run, log=log)
            if not dry_run:
                index.datasets.remove_location(dataset.id, uri)
            trash_count += 1

    _LOG.info("cleanup.finish", count=count, trash_count=trash_count)


def _as_utc(d):
    # UTC is default if not specified
    if d.tzinfo is None:
        return d.replace(tzinfo=tz.tzutc())
    return d.astimezone(tz.tzutc())


if __name__ == '__main__':
    main()
