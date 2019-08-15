#!/usr/bin/env python3

from __future__ import print_function

import os
import shutil
import tempfile
from contextlib import suppress
from pathlib import Path
from typing import Iterable

import click
import structlog
from boltons import fileutils
from eodatasets3 import verify

from datacube.index import Index
from datacube.model import Dataset
from datacube.ui import click as ui
from digitalearthau import paths as path_utils
from digitalearthau.collections import init_nci_collections, get_collections_in_path
from digitalearthau.paths import is_base_directory, BASE_DIRECTORIES, get_dataset_paths, split_path_from_base
from digitalearthau.uiutil import init_logging

_LOG = structlog.get_logger()


@click.command()
@ui.global_cli_options
@click.option('--dry-run', is_flag=True, default=False)
@click.option('--checksum/--no-checksum', is_flag=True, default=True)
@click.option('--destination', '-d',
              required=True,
              type=click.Path(exists=True, writable=True),
              help="Base folder where datasets will be placed inside")
@click.argument('paths',
                type=click.Path(exists=True, readable=True),
                nargs=-1)
@ui.pass_index('move')
def cli(index, dry_run, paths, destination, checksum):
    """
    Move the given folder of datasets into the given destination folder.

    This will checksum the data, copy it to the destination, and mark the original as archived in the DEA index.


    Notes:

    * An operator can later run dea-clean to trash the archived original locations.

    * Source datasets with failing checksums will be left as-is, with a warning logged.

    * Both the source(s) and destination paths are expected to be paths containing existing DEA collections.
    (See collections.py and paths.py)
    """
    init_logging()
    init_nci_collections(index)

    if not is_base_directory(destination):
        raise click.BadArgumentUsage(
            'Not a known DEA base directory; {}\nExpected one of:\n\t{}'.format(
                destination,
                '\n\t'.join(BASE_DIRECTORIES))
        )

    # We want to iterate all datasets in the given input folder, so we find collections that exist in
    # that folder and then iterate through all the collection datasets within that folder. Simple :)

    # We do this aggressively to find errors in arguments immediately. (with the downside of `paths` memory usage)
    resulting_paths = []
    for input_path in map(Path, paths):
        collections = list(get_collections_in_path(input_path))
        if not collections:
            raise click.BadArgumentUsage(f"Directory doesn't match any known collections: {input_path}")

        for collection in collections:
            resulting_paths.extend(list(collection.iter_fs_paths_within(input_path)))

    _LOG.info("dataset.count", input_count=len(paths), dataset_count=len(resulting_paths))

    # TODO: @ui.executor_cli_options
    move_all(
        index,
        resulting_paths,
        Path(destination),
        dry_run=dry_run,
        checksum=checksum
    )


def move_all(index: Index, paths: Iterable[Path], destination_base_path: Path, dry_run=False, checksum=True):
    for path in paths:
        mover = FileMover.evaluate_and_create(index, path, dest_base_path=destination_base_path)
        if not mover:
            continue

        mover.move(dry_run=dry_run, checksum=checksum)


class FileMover:
    """
    Move datasets around on the Filesystem, while keeping the DEA index up to date.

    The move is in terms of the index. Files on disk are copied, and it's a second, later
    step to remove them from their original location.

    There are several types of Datasets we may want to move around.

    1. A single file dataset. Eg. A *.nc file with an embedded `datasets` variable containing all metadata
    2. A directory dataset. Eg. Landsat scenes. A dataset is a directory containing multiple data files,
       a *-metadata.yaml file, and potentially other files as well.
    3. A dataset is a metadata file which lives beside a data file.
    4. A dataset is a metadata file which lives separately to a data file. (Probably not managed by DEA, unable to move)
    """
    def __init__(self,
                 source_path: Path,
                 dest_path: Path,
                 source_metadata_path: Path,
                 dest_metadata_path: Path,
                 dataset: Dataset,
                 index: Index) -> None:
        self.source_path = source_path
        self.dest_path = dest_path
        self.from_metadata_path = source_metadata_path
        self.dest_metadata_path = dest_metadata_path
        self.dataset = dataset

        self.index = index

        self.source_uri = self.from_metadata_path.as_uri()
        self.dest_uri = self.dest_metadata_path.as_uri()

        self.log = _LOG.bind(source_path=self.source_path)

        if not str(self.from_metadata_path).startswith(str(self.source_path)):
            # We only currently support copying when metadata is stored within the dataset.
            # Eg.
            # - an '.nc' file (same md path and dataset path)
            # - '*-metadata.yaml' file (inside the dataset path folder)
            raise NotImplementedError("Only metadata stored within a dataset is currently supported ")

    @classmethod
    def evaluate_and_create(cls, index: Index, path: Path, dest_base_path: Path):
        """
        Create a move task if this path is movable.
        """
        path = path.absolute()
        log = _LOG.bind(path=path)

        metadata_path = path_utils.get_metadata_path(path)
        log.debug("found.metadata_path", metadata_path=metadata_path)

        dataset_path, dest_path, dest_md_path = cls._compute_paths(metadata_path, dest_base_path)
        if dest_path.exists() or dest_md_path.exists():
            log.info("skip.exists", dest_path=dest_path)
            return None

        dataset_id = path_utils.get_path_dataset_id(metadata_path)
        log = log.bind(dataset_id=dataset_id)
        log.debug("found.dataset_id")

        dataset = index.datasets.get(dataset_id)
        log.debug('found.is_indexed', is_indexed=dataset is not None)
        # If it's not indexed in the cube yet, skip it. It's probably a new arrival.
        if not dataset:
            log.warn("skip.not_indexed")
            return None

        return FileMover(
            source_path=dataset_path,
            dest_path=dest_path,
            source_metadata_path=metadata_path,
            dest_metadata_path=dest_md_path,
            dataset=dataset,
            index=index
        )

    def move(self, dry_run=True, checksum=True):
        dest_metadata_uri = self._do_copy(dry_run=dry_run, checksum=checksum)
        if not dest_metadata_uri:
            self.log.debug("index.skip")
            return

        # Record destination location in index
        if not dry_run:
            self.index.datasets.add_location(self.dataset.id, uri=dest_metadata_uri)
        self.log.info('index.dest.added', uri=dest_metadata_uri)

        # Archive source file in index (for deletion soon)
        if not dry_run:
            self.index.datasets.archive_location(self.dataset.id, self.source_uri)

        self.log.info('index.source.archived', uri=self.source_uri)

    @staticmethod
    def _compute_paths(source_metadata_path, destination_base_path):
        dataset_path, all_files = get_dataset_paths(source_metadata_path)
        _, dataset_offset = split_path_from_base(dataset_path)
        new_dataset_location = destination_base_path.joinpath(dataset_offset)
        _, metadata_offset = split_path_from_base(source_metadata_path)
        new_metadata_location = destination_base_path.joinpath(metadata_offset)

        # We currently assume all files are contained in the dataset directory/path:
        # we write the single dataset path atomically.
        if not all(str(f).startswith(str(dataset_path)) for f in all_files):
            raise NotImplementedError("Some dataset files are not contained in the dataset path. "
                                      "Situation not yet implemented. %s" % dataset_path)

        return dataset_path, new_dataset_location, new_metadata_location

    def _do_copy(self, dry_run=True, checksum=True):
        log = self.log
        dest_path = self.dest_path
        dataset_path = self.source_path

        if checksum:
            successful_checksum = _verify_checksum(self.log, self.from_metadata_path,
                                                   dry_run=dry_run)
            self.log.info("checksum.complete", passes_checksum=successful_checksum)
            if not successful_checksum:
                raise RuntimeError("Checksum failure on " + str(self.from_metadata_path))

        if dataset_path.is_dir():
            self.copy_directory(dataset_path, dest_path, dry_run, log)
        elif self.dest_path == self.dest_metadata_path:  # Metadata is contained within the dataset file. eg. *.nc
            self.copy_file(dataset_path, dest_path, log)
        else:
            # Datasets that are dataset file + sibling or metadata separate to data
            raise NotImplementedError("TODO: dataset files not yet supported")

        return self.dest_uri

    def copy_file(self, from_, to, log):
        to_directory = to.parent
        log.debug("copy.mkdir", dest=to_directory)
        fileutils.mkdir_p(to.parent)
        # We don't want to risk partially-copied files left on disk, so we copy to a tmp name
        # then atomically rename into place.
        tmp_name = tempfile.mktemp(prefix='.dea-mv-', dir=to_directory)
        try:
            log.info("copy.put", src=from_, tmp_dest=tmp_name)
            shutil.copy(from_, tmp_name)
            log.debug("copy.put.done")
            os.rename(tmp_name, to)
        finally:
            log.debug('tmp_file.rm', tmp_file=tmp_name)
            with suppress(FileNotFoundError):
                os.remove(tmp_name)

    def copy_directory(self, from_, dest_path, dry_run, log):
        log.debug("copy.mkdir", dest=dest_path.parent)
        fileutils.mkdir_p(str(dest_path.parent))
        # We don't want to risk partially-copied packaged left on disk, so we copy to a tmp dir in same
        # folder and then atomically rename into place.
        tmp_dir = Path(tempfile.mkdtemp(prefix='.dea-mv-', dir=str(dest_path.parent)))
        try:
            tmp_package = tmp_dir.joinpath(from_.name)
            log.info("copy.put", src=from_, tmp_dest=tmp_package)
            if not dry_run:
                shutil.copytree(from_, tmp_package)
                log.debug("copy.put.done")
                os.rename(tmp_package, dest_path)
                log.debug("copy.rename.done")

                # It should have been contained within the dataset, see the check in the constructor.
                assert self.dest_metadata_path.exists()
        finally:
            log.debug("tmp_dir.rm", tmp_dir=tmp_dir)
            shutil.rmtree(tmp_dir, ignore_errors=True)


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

    log.debug("copy.verify", file_count=len(all_files))
    return True


def _expected_checksum_path(dataset_path):
    """
    :type dataset_path: pathlib.Path
    :rtype: pathlib.Path

    >>> import tempfile
    >>> tempdir = Path(tempfile.mkdtemp())
    >>> _expected_checksum_path(tempdir).name == 'package.sha1'
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
    cli()
