# coding=utf-8
"""
Sync a datacube index to a folder of datasets on disk.

Locations will be added/removed according to whether they're on disk, extra datasets will be indexed, etc.
"""
import sys
from pathlib import Path
from typing import Iterable

import click
import structlog
from boltons import strutils

from datacube.index._api import Index
from datacube.ui import click as ui
from datacubenci.archive import CleanConsoleRenderer
from datacubenci.collections import get_collection, registered_collection_names
from datacubenci.sync import scan
from datacubenci.sync.index import AgdcDatasetPathIndex
from . import fixes
from .differences import Mismatch

_LOG = structlog.get_logger()


@click.command()
@ui.global_cli_options
@click.option('--cache-folder',
              type=click.Path(exists=True, readable=True, writable=True),
              # 'cache' folder in current directory.
              default='cache')
@click.option('-j', '--jobs',
              type=int,
              default=2,
              help="Number of worker processes to use")
@click.option('-f',
              type=click.Path(exists=True, readable=True, dir_okay=False),
              help="Input from file instead of scanning collections")
@click.option('--index-missing', is_flag=True, default=False,
              help="Index on-disk datasets that have never been indexed")
@click.option('--trash-missing', is_flag=True, default=False,
              help="Trash on-disk datasets that have never been indexed")
@click.option('--update-locations', is_flag=True, default=False,
              help="Update the locations in the index to reflect locations on disk")
@click.option('--trash-archived', is_flag=True, default=False,
              help="Trash any files that were archived at least '--min-trash-age' hours ago")
@click.option('--min-trash-age-hours', is_flag=True, default=72, type=int,
              help="Minimum allowed archive age to trash a file")
# TODO
# @click.option('--validate', is_flag=True, default=False,
#               help="Run any available checksums or validation checks for the file type")
@click.option('-o',
              type=click.Path(writable=True, dir_okay=False),
              help="Output to file instead of stdout")
@click.argument('collections',
                type=click.Choice(registered_collection_names()),
                nargs=-1)
@ui.pass_index(expect_initialised=False)
def cli(index: Index, collections: Iterable[str], cache_folder: str, f: str, o: str,
        min_trash_age_hours: bool, jobs: int, **fix_settings):
    init_logging()

    if fix_settings['index_missing'] and fix_settings['trash_missing']:
        click.echo('Can either index missing datasets (--index-missing) , or trash them (--trash-missing), '
                   'but not both at the same time.', err=True)
        sys.exit(1)

    if f:
        mismatches = mismatches_from_file(Path(f))
    else:
        mismatches = scan.mismatches_for_collections(
            (get_collection(collection_name) for collection_name in collections),
            Path(cache_folder), index, workers=jobs
        )

    out_f = sys.stdout
    if o:
        out_f = open(o, 'w')

    def print_mismatch(mismatch):
        click.echo(
            '\t'.join(map(str, (
                # TODO: mismatch.collection:
                None,
                strutils.camel2under(mismatch.__class__.__name__),
                mismatch.dataset.id,
                mismatch.uri
            ))),
            file=out_f
        )

    try:
        with AgdcDatasetPathIndex(index, None) as path_index:
            fixes.fix_mismatches(mismatches, path_index,
                                 min_trash_age_hours=min_trash_age_hours,
                                 pre_fix=print_mismatch,
                                 **fix_settings)
    finally:
        if o:
            out_f.close()


def mismatches_from_file(f: Path):
    raise NotImplementedError("Loading mismatches from file")


def init_logging():
    # Direct structlog into standard logging.
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M.%S"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            # Coloured output if to terminal.
            CleanConsoleRenderer() if sys.stdout.isatty() else structlog.processors.KeyValueRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


if __name__ == '__main__':
    cli()
    print("Done", file=sys.stderr)
