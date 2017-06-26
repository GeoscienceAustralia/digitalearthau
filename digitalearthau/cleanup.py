"""
Find and trash archived locations.
"""

import click
import structlog

from datacube import Datacube
from datacube.ui import click as ui
from digitalearthau import paths

_LOG = structlog.getLogger('cleanup-archived')


@click.command(help='Find and trash archived locations for the given search terms.')
@click.option('--only-redundant/--all',
              is_flag=True,
              default=True,
              help='Only trash locations with a second active location')
@click.option('--dry-run',
              is_flag=True,
              help="Don't make any changes (ie. don't trash anything)")
@ui.parsed_search_expressions
def main(expressions, dry_run, only_redundant):
    with Datacube() as dc:
        _LOG.info('query', query=expressions)
        count = 0
        trash_count = 0
        for dataset in dc.index.datasets.search(**expressions):
            count += 1

            log = _LOG.bind(dataset_id=str(dataset.id))

            archived_uris = dc.index.datasets.get_archived_locations(dataset.id)
            if not archived_uris:
                continue

            if only_redundant:
                if dataset.uris is not None and len(dataset.uris) == 0:
                    # This active dataset has no active locations to replace the one's we're archiving.
                    # Probably a mistake? Don't trash the archived ones yet.
                    log.warning("dataset.noactive", archived_paths=archived_uris)
                    continue

            for uri in archived_uris:
                paths.trash_uri(uri, dry_run=dry_run, log=log)
                if not dry_run:
                    dc.index.datasets.remove_location(dataset.id, uri)
                trash_count += 1

        _LOG.info("cleanup.finish", count=count, trash_count=trash_count)


if __name__ == '__main__':
    main()
