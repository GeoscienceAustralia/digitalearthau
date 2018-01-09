from datetime import datetime, timedelta
from pathlib import Path

import click
import structlog
import sys

from click import echo, style
from dateutil import tz
from typing import Iterable, List

from sqlalchemy import select, or_, and_

from datacube.index._api import Index
from datacube.model import DatasetType, Dataset
from datacube.ui import click as ui
from digitalearthau import paths, uiutil

from datacube.index.postgres import _api as pgapi

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
@click.option('--only-redundant/--all',
              is_flag=True,
              default=True,
              help='Only trash locations with a second active location')
@ui.pass_index()
@click.argument('files',
                type=click.Path(exists=True, readable=True),
                nargs=-1)
def archived(index: Index,
             dry_run: bool,
             only_redundant: bool,
             files: List[str],
             min_trash_age_hours: int):
    """
    Cleanup locations that have been archived.
    """
    for f in files:
        p = Path(f)

        p_uri = p.absolute().as_uri()
        assert p_uri.startswith('file:')
        p_uri_body = p_uri.split('file:')[1]
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
                            pgapi.DATASET_LOCATION.c.uri_body.like(p_uri_body + '%')
                        )
                    )
                )
            ]

        # Multiple datasets can point to the same location (eg. a stacked file).
        # Check that there's no other active locations for this dataset.

        print(f"Matched {len(locations)} locations")
        for uri in locations:
            active_dataset = _get_active_dataset(index, uri)

            if active_dataset:
                print(f"\nSkipping {uri}: active exists: {active_dataset.id}\n")
            else:
                print(f"\nTrashing {uri}\n")


def _get_active_dataset(index, uri):
    datasets = list(index.datasets.get_datasets_for_location(uri))
    for d in datasets:
        if uri in d.uris:
            return d
    return None


@main.command('indexed')
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
    """
    Find and trash archived locations using an index search.

    But only if they were archived more than  --min-trash-age-hours ago (default: 3 days)

    By default it will also only trash a location if you've got another active location for it (as is
    the case if the archived location was stacked or dea-move'd)
    """
    echo(f"query: {expressions!r}", err=True)

    product_counts = {product.name: count for product, count in index.datasets.count_by_product(**expressions)}
    echo(f"{len(product_counts)} product(s) to scan; around {sum(product_counts.values())} datasets", err=True)

    # We only support cleaning one product on our pgbouncer, due to this bug:
    # https://github.com/opendatacube/datacube-core/pull/317
    # pylint: disable=protected-access
    if len(product_counts) > 1 and ('6543' in str(index.datasets._db.url)):
        echo("\nRunning against multiple products will fail on port 6432 at NCI until AGDC 1.5.4 is released.\n"
             "Change port to 5432 or limit your search arguments to one product.", err=True)
        sys.exit(1)

    latest_time_to_archive = _as_utc(datetime.utcnow()) - timedelta(hours=min_trash_age_hours)

    total_count = 0
    total_trash_count = 0

    for product, datasets in index.datasets.search_by_product(**expressions):
        expected_count = product_counts.get(product.name, 0)

        # Record results to the product's work folder.
        work_path = paths.get_product_work_directory(product.name, task_type='clean')
        uiutil.init_logging(work_path.joinpath('log.jsonl').open('a'))
        log = structlog.getLogger("cleanup-indexed").bind(product=product.name)
        echo(f"Cleaning {style(product.name, bold=True)}", err=True)
        echo(f"  Expect {expected_count} datasets", err=True)
        echo(f"  Output {work_path}", err=True)

        log.info('arguments', arguments=dict(
            query=expressions,
            dry_run=dry_run,
            only_redundant=only_redundant,
            min_trash_age_hours=min_trash_age_hours
        ))
        count, trash_count = _cleanup_datasets(
            # Yuck. Too many arguments.
            index, product, datasets, expected_count, dry_run, latest_time_to_archive, only_redundant, log
        )
        total_count += count
        total_trash_count += trash_count

    echo(f"Finished {total_count} datasets; {total_trash_count} trashed.", err=True)


def _cleanup_datasets(index: Index,
                      product: DatasetType,
                      datasets: Iterable[Dataset],
                      expected_count: int,
                      dry_run: bool,
                      latest_time_to_archive: datetime,
                      only_redundant: bool,
                      log):
    count = 0
    trash_count = 0

    log.info("cleanup.product.start", expected_count=expected_count, product=product.name)

    with click.progressbar(datasets,
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
                    # This active dataset has no active locations to replace the ones we're archiving.
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
    return count, trash_count


@main.command('files')
@ui.pass_index()
@click.argument('files',
                type=click.Path(exists=True, readable=True),
                nargs=-1)
@TRASH_OPTIONS
def trash_individual_files(index, dry_run, min_trash_age_hours, files):
    """
    Trash the given datasets if they're archived.

    This expects exact dataset paths: *.nc files, or ga-metadata.yaml for scenes.

    But only if they were archived more than  --min-trash-age-hours ago (default: 3 days)
    """
    glog = structlog.getLogger('cleanup-paths')
    glog.info('input_paths', input_paths=files)

    latest_time_to_archive = _as_utc(datetime.utcnow()) - timedelta(hours=min_trash_age_hours)

    count = 0
    trash_count = 0
    for file in files:
        count += 1
        log = glog.bind(path=(Path(file).resolve()))

        uri = Path(file).resolve().as_uri()
        datasets = list(index.datasets.get_datasets_for_location(uri))

        if datasets:
            # Can't trash file if any linked datasets are still active. They should be archived first.
            active_dataset_ids = [dataset.id for dataset in datasets if dataset.is_active]
            if active_dataset_ids:
                log.warning('dataset.is_active', dataset_ids=active_dataset_ids)
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

    glog.info("cleanup.finish", count=count, trash_count=trash_count)


def _as_utc(d):
    # UTC is default if not specified
    if d.tzinfo is None:
        return d.replace(tzinfo=tz.tzutc())
    return d.astimezone(tz.tzutc())


if __name__ == '__main__':
    main()
