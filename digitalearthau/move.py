#!/usr/bin/env python3

from __future__ import print_function

import os
import shutil
import sys
import tempfile
from pathlib import Path

import click
import structlog
from boltons import fileutils
from eodatasets import verify

from datacube.index._api import Index
from datacube.model import Dataset
from datacube.ui import click as ui
from digitalearthau import paths as path_utils
from digitalearthau.mdss import MDSSClient

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
    move_all(
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


def move_all(index, project, paths, dry_run=False):
    """
    :param str project: Which NCI project's mdss space to use
    :type index: datacube.index._api.Index
    :type dry_run: bool
    :param paths:
    """
    if not MDSSClient.is_available():
        raise RuntimeError("mdss is not available on this system")

    for path in paths:
        task = FileMoveTask.evaluate_and_create(index, project, path)
        if not task:
            continue

        _archive_path(index, task, dry_run=dry_run)


class FileMoveTask:
    def __init__(self, source_path: Path, dest_path: Path, source_metadata_path: Path, dataset: Dataset, log=_LOG):
        self.source_path = source_path
        self.source_metadata_path = source_metadata_path
        self.dataset = dataset
        self.log = log.bind(path=source_path)
        self.dest_path = dest_path

    @property
    def source_uri(self):
        return self.source_path.as_uri()

    def status(self, **vals):
        self.log = self.log.bind(**vals)

    @classmethod
    def evaluate_and_create(cls, index: Index, path: Path, dest_path: Path):
        """
        Create a move task if this path is movable.
        """
        path = path.absolute()
        log = _LOG.bind(input_path=path)

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

        return FileMoveTask(
            source_path=path,
            dest_path=dest_path,
            source_metadata_path=metadata_path,
            dataset=dataset,
            log=log
        )

    def run(self, dry_run=True):
        dataset_path, all_files = path_utils.get_dataset_paths(self.source_metadata_path)

        log = self.log
        assert all_files
        assert all(f.is_file() for f in all_files)
        log.debug("copy.verify", file_count=len(all_files))
        _, file_offset = path_utils.split_path_from_base(dataset_path)
        new_dataset_location = self.dest_path.joinpath(file_offset)

        if new_dataset_location.exists():
            raise NotImplementedError(
                "Destination already exists. Situation not implemented. %s" % new_dataset_location)

        if dataset_path.is_dir():
            # Assume all files are in the dataset directory: we'll write the whole folder atomically.
            if not all(str(f).startswith(str(dataset_path)) for f in all_files):
                raise NotImplementedError("Some dataset files are not contained in the dataset folder. "
                                          "Situation not implemented. %s" % dataset_path)

            log.debug("copy.mkdir", dest=new_dataset_location.parent)
            fileutils.mkdir_p(new_dataset_location.parent)

            # We don't want to risk partially-copied packaged left on disk, so we copy to a tmp dir in same
            # folder and then atomically rename into place.
            tmp_dir = tempfile.mkdtemp(prefix='.dea-mv-', dir=str(new_dataset_location.parent))
            try:
                tmp_package = tmp_dir.join(dataset_path.name)
                log.info("copy.put", src=dataset_path, tmp_dest=tmp_package)
                if not dry_run:
                    shutil.copy2(str(dataset_path), str(tmp_package))
                    log.debug("copy.put.done")
                    os.rename(str(tmp_package), str(new_dataset_location))
                    log.debug("copy.rename.done")
            finally:
                log.debug("tmp_dir.rm", tmp_dir=tmp_dir)
                shutil.rmtree(tmp_dir, ignore_errors=True)
        else:
            # .nc files and sibling files
            raise NotImplementedError("TODO: dataset files not yet supported")

        return new_dataset_location.as_uri()


def _archive_path(index, task, target_dir, dry_run=True):
    """
    :type index: datacube.index._api.Index
    :type task: FileMoveTask
    :type dry_run: bool
    """
    successful_checksum = _verify_checksum(task.log, task.source_metadata_path,
                                           dry_run=dry_run)
    task.log.info("checksum.complete", passes_checksum=successful_checksum)
    if not successful_checksum:
        raise RuntimeError("Checksum failure on " + str(task.source_metadata_path))

    dest_uri = task.run(dry_run=dry_run)

    # Record destination location in index
    if not dry_run:
        index.datasets.add_location(task.dataset, uri=dest_uri)
    task.log.info('index.dest.added', uri=dest_uri)

    # Archive source file in index (for deletion soon)
    if not dry_run:
        index.datasets.archive_location(task.dataset, task.source_uri)
    task.log.info('index.source.archived', uri=task.source_uri)


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


if __name__ == '__main__':
    main()
