#!/usr/bin/env python

"""
For given dataset ids, remove the locations that don't exist.
"""
from __future__ import print_function

import click
from datacube import utils
from datacube.ui import click as ui


# pylint: disable=protected-access

@click.command()
@ui.global_cli_options
@click.option('--dry-run', is_flag=True, default=False)
@click.argument('ids', type=str, nargs=-1)
@ui.pass_index()
def main(index, dry_run, ids):
    exists = 0
    missing = 0
    removed = 0

    missing_datasets = []
    for dataset_id in ids:
        with index._db.begin() as db:
            some_exist = False
            to_remove = []
            for uri in db.get_locations(dataset_id):
                local_path = utils.uri_to_local_path(uri)

                if local_path.exists():
                    exists += 1
                    some_exist = True
                    print("Exists: {}".format(uri))
                    continue
                else:
                    missing += 1
                    to_remove.append(uri)
            # If there are some valid locations, remove invalid ones.
            if some_exist:
                for uri in to_remove:
                    was_removed = db.remove_location(dataset_id, uri) if not dry_run else False
                    print("Removed ({}): {}".format(was_removed, uri))
                    if was_removed:
                        removed += 1
            # If there's no valid locations, note them for later
            else:
                missing_datasets.append(dataset_id)

    print("Done: skipped {}, missing {}, removed {}".format(exists, missing, removed))
    print("{} without locations:".format(len(missing_datasets)))
    if missing_datasets:
        print("\t" + ("\n\t".join(missing_datasets)))


if __name__ == '__main__':
    main()
