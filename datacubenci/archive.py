#!/usr/bin/env python3

from __future__ import print_function

import tarfile
import tempfile
from pathlib import Path

import click

import os

import shutil

import sys

from datacube.ui import click as ui
from datacubenci import paths as path_utils
from datacubenci.mdss import MDSSClient

from eodatasets import verify
import structlog

_LOG = structlog.get_logger()


@click.command()
@ui.global_cli_options
@click.option('--project', required=True)
@click.option('--dry-run', is_flag=True, default=False)
@click.argument('paths', type=str, nargs=-1)
@ui.pass_index('mdss-archival')
def main(index, project, dry_run, paths):
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M.%S"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            # Coloured output if to terminal.
            CleanConsoleRenderer() if sys.stdout.isatty() else structlog.processors.JSONRenderer(),
        ],
        context_class=dict,
        cache_logger_on_first_use=True,
    )

    # TODO: @ui.executor_cli_options
    archive_all(
        index,
        project,
        [Path(path).absolute() for path in paths],
        dry_run=dry_run,
    )


class CleanConsoleRenderer(structlog.dev.ConsoleRenderer):
    def __init__(self, pad_event=25):
        super().__init__(pad_event)
        # Dim debug messages
        self._level_to_color['debug'] = structlog.dev.DIM


def archive_all(index, project, paths, dry_run=False):
    """
    :param str project: Which NCI project's mdss space to use
    :type index: datacube.index._api.Index
    :type dry_run: bool
    :param paths:
    """
    if not MDSSClient.is_available():
        raise RuntimeError("mdss is not available on this system")

    for path in paths:
        task = MdssMoveTask.evaluate_and_create(index, project, path)
        if not task:
            continue

        _archive_path(index, task, dry_run=dry_run)


class MdssMoveTask:
    def __init__(self, source_path, project, source_metadata_path, dataset, log=_LOG):
        self.source_path = source_path
        self.project = project
        self.source_metadata_path = source_metadata_path
        self.dataset = dataset
        self.log = log.bind(path=source_path)

    @property
    def source_uri(self):
        return self.source_path.as_uri()

    def status(self, **vals):
        self.log = self.log.bind(**vals)

    @classmethod
    def evaluate_and_create(cls, index, project, path):
        """
        Create a move task if this path is movable.
        """
        path = path.absolute()
        log = _LOG.bind(input_path=path, project=project)

        metadata_path = path_utils.get_metadata_path(path)
        log.debug("found.metadata_path", metadata_path=metadata_path)

        dataset_id = path_utils.get_path_dataset_id(metadata_path)
        log = log.bind(dataset_id=dataset_id)
        log.debug("found.dataset_id")

        dataset = index.datasets.get(dataset_id)
        log.debug('found.is_indexed', is_indexed=dataset is not None)
        # If it's not indexed in the cube yet, skip it. It's probably a new arrival.
        if not dataset:
            log.warn("skip.not_indexed")
            return None

        # Count how many datasets have been processed from this one. If none, we don't want to archive yet.
        derived_count = len(tuple(index.datasets.get_derived(dataset_id)))
        log.debug("found.derived", derived_count=derived_count)
        if not derived_count:
            log.info("skip.no_derived_datasets")
            return None

        return MdssMoveTask(
            source_path=path,
            project=project,
            source_metadata_path=metadata_path,
            dataset=dataset,
            log=log
        )


def _archive_path(index, task, dry_run=True):
    """
    :type index: datacube.index._api.Index
    :type task: MdssMoveTask
    :type dry_run: bool
    """
    successful_checksum = _verify_checksum(task.log, task.source_metadata_path,
                                           dry_run=dry_run)
    task.log.info("checksum.complete", passes_checksum=successful_checksum)
    if not successful_checksum:
        raise RuntimeError("Checksum failure on " + str(task.source_metadata_path))

    mdss_uri = _copy_to_mdss(task.log, task.source_metadata_path, task.dataset.id, task.project,
                             dry_run=dry_run)

    # Record mdss tar location in index
    if not dry_run:
        index.datasets.add_location(task.dataset, uri=mdss_uri)
    task.log.info('index.mdss.removed', uri=mdss_uri)

    # Remove local file from index
    if not dry_run:
        index.datasets.remove_location(task.dataset, task.source_uri)
    task.log.info('index.source.removed', uri=task.source_uri)


def _verify_checksum(log, metadata_path, dry_run=True):
    dataset_path, all_files = path_utils.get_dataset_paths(metadata_path)
    checksum_file = _expected_checksum_path(dataset_path)
    if not checksum_file.exists():
        # Ingested data doesn't currently have them, so it's only a warning.
        log.warning("checksum.missing", checksum_file=checksum_file)
        return None

    ch = verify.PackageChecksum()
    ch.read(checksum_file)
    if not dry_run:
        for file, successful in ch.iteratively_verify():
            if successful:
                log.debug("checksum.pass", file=file)
            else:
                log.error("checksum.failure", file=file)
                return False

    return True


def _expected_checksum_path(dataset_path):
    """
    :type dataset_path: pathlib.Path
    :rtype: pathlib.Path

    >>> import tempfile
    >>> _expected_checksum_path(Path(tempfile.mkdtemp())).name == 'package.sha1'
    True
    >>> file_ = Path(tempfile.mktemp(suffix='-dataset-file.tif'))
    >>> file_.open('a').close()
    >>> file_chk = _expected_checksum_path(file_)
    >>> str(file_chk).endswith('-dataset-file.tif.sha1')
    True
    >>> file_chk.parent == file_.parent
    True
    """
    if dataset_path.is_dir():
        return dataset_path.joinpath('package.sha1')

    return dataset_path.parent.joinpath(dataset_path.name + '.sha1')


def _copy_to_mdss(log, metadata_path, dataset_id, destination_project, dry_run=True):
    dataset_path, all_files = path_utils.get_dataset_paths(metadata_path)
    assert all_files
    assert all(f.is_file() for f in all_files)
    log.debug("mdss.verify", file_count=len(all_files))

    _, dataset_path_offset = path_utils.split_path_from_base(dataset_path.parent)
    mdss_location = "agdc-archive/{file_postfix}".format(file_postfix=dataset_path_offset)
    log = log.bind(mdss_location=mdss_location)
    log.debug("mdss.location")

    mdss = MDSSClient(destination_project)

    if not dry_run:
        # Destination MDSS offset: the data path minus the trash path prefix. (?)
        tmp_dir = tempfile.mkdtemp(prefix='mdss-transfer-{}-'.format(str(dataset_id)))
        try:
            transferable_paths = _get_transferable_paths(log, all_files, dataset_path, tmp_dir)

            log.debug("mdss.mkdir")
            mdss.make_dirs(mdss_location)
            log.info("mdss.put", transferable_paths=transferable_paths)
            mdss.put(transferable_paths, mdss_location)
            log.debug("mdss.put.done")
        finally:
            log.debug("tmp_dir.rm", tmp_dir=tmp_dir)
            shutil.rmtree(tmp_dir, ignore_errors=True)

    return mdss.to_uri(mdss_location)


def _get_transferable_paths(log, all_files, dataset_path, tmp_dir):
    # If it's two files or less, copy them directly
    if len(all_files) <= 2:
        log.debug("")
        return all_files

    # Otherwise tar all files
    tar_path = os.path.join(tmp_dir, str(dataset_path.name) + '.tar')
    log = log.bind(tar_path=tar_path)
    log.debug("tar.create")
    with tarfile.open(str(tar_path), "w") as tar:
        for path in all_files:
            log.debug("tar.add_file", file=path)
            tar.add(str(path), arcname=str(path.relative_to(dataset_path)))

    log.debug("tar.done")
    return [tar_path]


if __name__ == '__main__':
    main()
