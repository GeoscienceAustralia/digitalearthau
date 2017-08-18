"""
Find and trash archived locations of active datasets.
"""
from datetime import datetime, timedelta
from pathlib import Path

import click
import structlog
from dateutil import tz

from datacube.ui import click as ui
from digitalearthau import paths, uiutil

_LOG = structlog.getLogger('cleanup-archived')

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


@main.command('indexed', help="Search the index for archived locations")
@TRASH_OPTIONS
@click.option('--only-redundant/--all',
              is_flag=True,
              default=True,
              help='Only trash locations with a second active location')
@ui.pass_index()
@ui.parsed_search_expressions
def indexed(index, expressions, dry_run, only_redundant, min_trash_age_hours):
    uiutil.init_logging()

    _LOG.info('query', query=expressions)
    datasets = index.datasets.search(**expressions)

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


@main.command('files', help="""Trash the given dataset paths directly.

But only if all indexed datasets are archived
""")
@ui.pass_index()
@click.argument('files',
                type=click.Path(exists=True, readable=True),
                nargs=-1)
@TRASH_OPTIONS
def trash_individual_files(index, dry_run, min_trash_age_hours, files):
    _LOG.info('input_paths', input_paths=files)

    latest_time_to_archive = _as_utc(datetime.utcnow()) - timedelta(hours=min_trash_age_hours)

    count = 0
    trash_count = 0
    for file in files:
        count += 1
        path = Path(file).absolute()
        log = _LOG.bind(path=path)

        uri = path.as_uri()
        datasets = list(index.datasets.get_datasets_for_location(uri))

        if datasets:
            # Can't trash file if any linked datasets are still active. They should be archived first.
            active_dataset_ids = [dataset.id for dataset in datasets if dataset.is_active]
            if active_dataset_ids:
                _LOG.warning('dataset.is_active', dataset_ids=active_dataset_ids)
                continue

            assert all(d.is_archived for d in datasets)

            # Otherwise they're indexed and archived. Were they archived long enough ago?

            # It's rare that you'd have two archived datasets with the same location, but we're handling it anyway...
            archived_times = [dataset.archived_time for dataset in datasets]
            archived_times.sort()
            oldest_archived_time = archived_times[0]
            if _as_utc(oldest_archived_time) > latest_time_to_archive:
                log.info('dataset.too_recent', archived_time=oldest_archived_time)
                continue

            if not dry_run:
                for d in datasets:
                    log.info('dataset.remove_location', dataset_id=d.id)
                    index.datasets.remove_location(d.id, uri)
        else:
            log.info('path.not_indexed')
        paths.trash_uri(uri, dry_run=dry_run, log=log)
        trash_count += 1

    _LOG.info("cleanup.finish", count=count, trash_count=trash_count)


def _as_utc(d):
    # UTC is default if not specified
    if d.tzinfo is None:
        return d.replace(tzinfo=tz.tzutc())
    return d.astimezone(tz.tzutc())


if __name__ == '__main__':
    main()
