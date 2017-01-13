#!/usr/bin/env python

from __future__ import print_function

import tarfile
import tempfile

import click
import logging

import os

import shutil

from pathlib import Path
from datacube.ui import click as ui, common
from datacubenci import paths as path_utils
from datacubenci.mdss import MDSSClient

_LOG = logging.getLogger(__name__)


@click.command()
@ui.global_cli_options
@click.option('--project', required=True)
@click.option('--dry-run', is_flag=True, default=False)
@click.argument('paths', type=str, nargs=-1)
@ui.pass_index('mdss-archival')
def main(index, project, dry_run, paths):
    # TODO: @ui.executor_cli_options
    for path in paths:
        _move_path(index, project, path, dry_run=dry_run)


def _move_path(index, destination_project, path, dry_run=False):
    """
    :type index: datacube.index._api.Index
    :type path: pathlib.Path
    :type dry_run: bool
    """
    metadata_path = common.get_metadata_path(path)
    uri = Path(metadata_path).as_uri()
    dataset_id = path_utils.get_path_dataset_id(path)

    dataset = index.datasets.get(dataset_id)
    if not dataset:
        _LOG.info("No indexed (%s): %s", dataset_id, path)
        return

    derived_datasets = tuple(index.datasets.get_derived(dataset_id))
    if not derived_datasets:
        _LOG.info("Nothing has been derived, skipping (%s): %s", dataset_id, path)
        return

    # TODO: Verify checksums

    mdss_uri = _copy_to_mdss(metadata_path, dataset_id, destination_project)

    # Record mdss tar in index
    index.datasets.add_location(dataset, uri=mdss_uri)

    # Remove local file from index
    index.datasets.remove_location(dataset, uri)
    # TODO: Trash data_paths a few hours/days later?


def _copy_to_mdss(metadata_path, dataset_id, destination_project):
    mdss = MDSSClient(destination_project)

    dataset_path, all_files = path_utils.get_dataset_paths(metadata_path)
    assert all_files
    assert all(f.is_file() for f in all_files)

    # Destination MDSS offset: the data path minus the trash path prefix. (?)
    tmp_dir = tempfile.mkdtemp(suffix='mdss-transfer-{}'.format(str(dataset_id)))
    try:
        transferable_paths = _get_transferable_paths(all_files, dataset_path, tmp_dir)

        _, dataset_path_offset = path_utils.split_path_from_base(dataset_path.parent)
        dest_location = "agdc-archive/{file_postfix}".format(file_postfix=dataset_path_offset)
        mdss.make_dirs(dest_location)
        mdss.put(transferable_paths, dest_location)

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    return mdss.to_uri(dest_location)


def _get_transferable_paths(all_files, dataset_path, tmp_dir):
    # If it's two files or less, copy them directly
    if len(all_files) <= 2:
        return all_files

    # Otherwise tar all files
    tar_path = os.path.join(tmp_dir, str(dataset_path.name) + '.tar')
    with tarfile.open(tar_path, "w") as tar:
        for path in all_files:
            tar.add(path)
    return [tar_path]


if __name__ == '__main__':
    main()
