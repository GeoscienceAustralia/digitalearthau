# coding=utf-8
"""
Sync a datacube index to a folder of datasets on disk.

Locations will be added/removed according to whether they're on disk, extra datasets will be indexed, etc.
"""
import sys
from pathlib import Path
from typing import Iterable, List, Tuple

import click
import structlog

import digitalearthau.collections as cs
from datacube.index import Index
from datacube.ui import click as ui
from digitalearthau import uiutil
from digitalearthau.sync import scan
from . import fixes, differences
from .differences import Mismatch

_LOG = structlog.get_logger()


# This check is buggy when used with Tuple[] type: https://github.com/PyCQA/pylint/issues/867
# pylint: disable=invalid-sequence-index


@click.command('dea-sync')
@ui.global_cli_options
@click.option('--cache-folder',
              type=click.Path(exists=True, readable=True, writable=True),
              # 'cache' folder in current directory.
              default='cache')
@click.option('-j', '--jobs',
              type=int,
              default=4,
              help="Number of worker processes to use")
@click.option('-f', '--format', 'format_',
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
@click.option('-o', '--output', 'output_file',
              type=click.Path(writable=True, dir_okay=False),
              help="Output to file instead of stdout")
@click.argument('collection_specifiers',
                # help = "Either names of collections or subfolders of collections"
                nargs=-1, )
@ui.pass_index(expect_initialised=False)
def cli(index: Index,
        collection_specifiers: Iterable[str],
        cache_folder: str,
        format_: str,
        output_file: str,
        min_trash_age_hours: bool,
        jobs: int,
        **fix_settings):
    """
    Update a datacube index to the state of the filesystem.

    This will update locations, trash or index new datasets, depending on the chosen options.
    """
    uiutil.init_logging()

    if fix_settings['index_missing'] and fix_settings['trash_missing']:
        click.echo('Can either index missing datasets (--index-missing) , or trash them (--trash-missing), '
                   'but not both at the same time.', err=True)
        sys.exit(1)

    cs.init_nci_collections(index)

    mismatches = get_mismatches(cache_folder, collection_specifiers, format_, jobs)

    out_f = sys.stdout
    try:
        if output_file:
            out_f = open(output_file, 'w')

        fixes.fix_mismatches(
            mismatches,
            index,
            min_trash_age_hours=min_trash_age_hours,
            **fix_settings
        )
    finally:
        if output_file:
            out_f.close()


def resolve_collections(collection_specifiers: Iterable[str]) -> List[Tuple[cs.Collection, str]]:
    """
    >>> cs.init_nci_collections(None)
    >>> [(c.name, p) for c, p in resolve_collections(['ls8_level1_scene'])]
    [('ls8_level1_scene', 'file:///')]
    >>> [(c.name, p) for c, p in resolve_collections(['/g/data/v10/repackaged/rawdata/0/2015'])]
    [('telemetry', 'file:///g/data/v10/repackaged/rawdata/0/2015')]
    >>> [(c.name, p) for c, p in resolve_collections(['/g/data/v10/reprocess/ls7/level1'])]
    [('ls7_level1_scene', 'file:///g/data/v10/reprocess/ls7/level1')]
    >>> level1_folder_match = resolve_collections(['/g/data/v10/reprocess'])
    >>> sorted(c.name for c, p in level1_folder_match)
    ['ls5_level1_scene', 'ls7_level1_scene', 'ls8_level1_scene']
    >>> set(p for c, p in level1_folder_match)
    {'file:///g/data/v10/reprocess'}
    >>> resolve_collections(['/some/fake/path'])
    Traceback (most recent call last):
    ...
    ValueError: Matches no collections: '/some/fake/path'
    >>> # Just the prefix, not the whole complete folder name
    >>> [(c.name, p) for c, p in resolve_collections(['/g/data/v10/repackaged/rawdata/0/20'])]
    Traceback (most recent call last):
    ...
    ValueError: Matches no collections: '/g/data/v10/repackaged/rawdata/0/20'
    """
    out = []
    for spec in collection_specifiers:
        # Either a collection name or a path on the filesystem

        possible_path = Path(spec).absolute()

        collection = cs.get_collection(spec)
        collections_in_path = list(cs.get_collections_in_path(possible_path))

        # If it matches both, throw an error
        if collections_in_path and collection is not None:
            raise ValueError("Ambiguous input: %r is both a "
                             "collection name and a path on the filesystem" % (spec,))

        if collection:
            out.append((collection, 'file:///'))
        elif collections_in_path:
            for match in collections_in_path:
                out.append((match, possible_path.as_uri()))
        else:
            raise ValueError("Matches no collections: %r" % spec)

    return out


def get_mismatches(cache_folder: str,
                   collection_specifiers: Iterable[str],
                   input_file: str,
                   job_count: int):
    if input_file:
        yield from differences.mismatches_from_file(Path(input_file))
    else:
        for collection, uri_prefix in resolve_collections(collection_specifiers):
            yield from scan.mismatches_for_collection(
                collection,
                Path(cache_folder),
                uri_prefix=uri_prefix,
                workers=job_count
            )


if __name__ == '__main__':
    cli()
    print("Done", file=sys.stderr)
