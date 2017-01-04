#!/usr/bin/env python

from __future__ import print_function

import tarfile
import tempfile

import click
import logging

import os

import shutil

from subprocess import call
from pathlib import Path
from datacube.ui import click as ui

_LOG = logging.getLogger('move_to_mdss')


@click.command()
@ui.global_cli_options
@click.option('--project', required=True)
@click.option('--dry-run', is_flag=True, default=False)
@click.argument('paths', type=str, nargs=-1)
@ui.pass_index('mdss-archival')
def main(index, project, dry_run, paths):
    # TODO: @ui.executor_cli_options
    for path in paths:
        move_path(index, project, path, dry_run=dry_run)


def move_path(index, destination_project, path, dry_run=False):
    """
    :type index: datacube.index._api.Index
    :type path: pathlib.Path
    :type dry_run: bool
    """
    uri = Path(path).as_uri()
    dataset = dataset_from_path(path)

    if not index.datasets.has(dataset.id):
        _LOG.info("No indexed (%s): %s", dataset.id, path)
        return

    derived_datasets = tuple(index.datasets.get_derived(dataset.id))
    if not derived_datasets:
        _LOG.info("Nothing has been derived, skipping (%s): %s", dataset.id, path)
        return

    # TODO: Verify checksums

    data_paths = dataset_data_path(dataset)

    mdss = MDSSClient(destination_project)

    # Destination MDSS offset: the data path minus the trash path prefix. (?)
    dest_location = "agdc-archive/{file_postfix}"

    tmp_dir = tempfile.mkdtemp(suffix='mdss-transfer-{}'.format(str(dataset.id)))
    try:
        # If it's one file, copy it directly
        if len(data_paths) == 1 and not os.path.isdir(data_paths[0]):
            source_path = data_paths[0]
        # Otherwise tar it.
        else:
            tar_path = os.path.join(tmp_dir, str(dataset.id) + '.tar')
            with tarfile.open(tar_path, "w") as tar:
                for path in data_paths:
                    tar.add(path)
            source_path = tar_path

        mdss.make_dirs(dest_location)
        mdss.put(source_path, dest_location)

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    dest_uri = mdss.to_uri(dest_location)

    # Record mdss tar in index
    index.datasets.add_location(dataset, uri=dest_uri)

    # Remove local file
    index.datasets.remove_location(dataset, uri)
    # TODO: Trash data_paths a few hours/days later?


if __name__ == '__main__':
    main()

MDSS = "mdss -P v27"
DEST_ROOT = "ALOS/L0"


class MDSSClient(object):
    def __init__(self, project):
        self.project = project

    def _call(self, *args):
        base_args = ['mdss', '-P', self.project]
        base_args.extend(args)
        return call(base_args)

    def put(self, source_path, dest_path):
        retcode = self._call('put', source_path, dest_path)
        if retcode == -1:
            raise RuntimeError("Failed to transfer to {} MDSS: {} -> {}".format(self.project, source_path, dest_path))

    def make_dirs(self, path):
        if self._call('ls', '-d', path) != 0:
            if self._call('mkdir', path) != 0:
                raise RuntimeError("Failed to mkdir on MDSS {} at {}".format(self.project, path))

    def to_uri(self, path):
        return 'mdss://{project}/{offset}'.format(
            project=self.project,
            offset=path,
        )
