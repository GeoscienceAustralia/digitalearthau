"""
Find and trash archived locations of active datasets.
"""
from datetime import datetime, timedelta
from pathlib import Path

import click
import structlog
import sys

from click import echo
from dateutil import tz

from datacube.index._api import Index
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
def indexed(index: Index,
            expressions: dict,
            dry_run: bool,
            only_redundant: bool,
            min_trash_age_hours: int):
    uiutil.init_logging()

    _LOG.info('query', query=expressions)

    product_counts = {product.name: count for product, count in index.datasets.count_by_product(**expressions)}

    echo(f"{len(product_counts)} product(s) to scan; around {sum(product_counts.values())} datasets", err=True)

    latest_time_to_archive = _as_utc(datetime.utcnow()) - timedelta(hours=min_trash_age_hours)

    total_count = 0
    total_trash_count = 0

    for product, datasets in index.datasets.search_by_product(**expressions):
        count = 0
        trash_count = 0
        expected_count = product_counts[product.name]
        log = _LOG.bind(product=product.name)
        _LOG.info("cleanup.product.start", expected_count=expected_count, product=product.name)

        with click.progressbar(datasets,
                               label=f'{product.name} cleanup',
                               length=expected_count,
                               # stderr should be used for runtime information, not stdout
                               file=sys.stderr) as dataset_iter:
            for dataset in dataset_iter:
                count += 1

                log = log.bind(dataset_id=str(dataset.id))

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

                log = log.unbind('dataset_id')

        log.info("cleanup.product.finish", count=count, trash_count=trash_count)
        total_count += count
        total_trash_count += trash_count

    _LOG.info("cleanup.finish", count=total_count, trash_count=total_trash_count)


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
