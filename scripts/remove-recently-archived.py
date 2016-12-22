#!/usr/bin/env python

"""
This takes a csv of datasets (typically generated with find-deletable-archived-datasets.sh)
and moves the file to a trash folder if it exists.
"""
from __future__ import print_function

import os

from pathlib import Path
import csv
import sys

TRASH_ROOTS = (
    '/g/data/fk4/datacube',
    '/g/data/rs0/datacube',
    '/g/data/v10/reprocess',
    '/g/data/rs0/scenes/pq-scenes-tmp',
    '/g/data/rs0/scenes/nbar-scenes-tmp',
)


def main():
    if sys.argv[1] == "--perform":
        dry_run = False
        csv_file = sys.argv[2]
    else:
        dry_run = True
        csv_file = sys.argv[1]

    delete_count = 0
    with open(csv_file, 'r') as c:
        reader = csv.reader(c)

        missing_count = 0
        nonlocal_count = 0
        for row in reader:
            if row[0] != 'file':
                print("Skipping non-file uri: {}:{}".format(row[0], row[1]))
                nonlocal_count += 1
                continue
            file_path = Path(row[1])

            if not file_path.exists():
                missing_count += 1
                continue

            data_path = to_data_path(file_path)

            trash_path = get_trash_path(data_path)

            print("Archiving {}".format(data_path))
            print("       to {}".format(trash_path))
            delete_count += 1
            if not dry_run:
                if not trash_path.parent.exists():
                    os.makedirs(str(trash_path.parent))
                os.rename(str(data_path), str(trash_path))

    print()
    print("{} deletable, {} already gone, {} non-local".format(delete_count, missing_count, nonlocal_count))


def get_trash_path(file_path):
    for trash_root in TRASH_ROOTS:
        if str(file_path).startswith(trash_root):
            dir_offset = str(file_path)[len(trash_root) + 1:]
            return Path(trash_root).joinpath('.trash', dir_offset)

    raise ValueError("Unknown location: no trash directory: " + str(file_path))


def to_data_path(file_path):
    if file_path.suffix == '.nc':
        return file_path
    if file_path.name == 'ga-metadata.yaml':
        return file_path.parent

    raise ValueError("Unsupported path type: " + str(file_path))


if __name__ == '__main__':
    main()
