#!/usr/bin/env python

from pathlib import Path

import click
import structlog

from datacube.index.index import Index
from datacube.ui import click as ui
from digitalearthau import paths

_LOG = structlog.get_logger()


@click.command()
@ui.config_option
@click.option('--dry-run', is_flag=True, default=False)
@ui.pass_index(expect_initialised=False)
@click.argument('trash_path', type=click.Path(exists=True, readable=True, writable=True))
def restore(index: Index, trash_path: str, dry_run: bool):
    trash_base = Path(trash_path)
    assert trash_base.exists()

    for trashed_nc in trash_base.rglob('L*.nc'):
        restorable_path = _should_restore(index, trashed_nc)

        if restorable_path:
            _LOG.info("trash.restore", trash_path=trashed_nc, original_path=restorable_path)
            if not dry_run:
                Path(trashed_nc).rename(restorable_path)


def _should_restore(index, trashed_nc):
    dataset_ids = paths.get_path_dataset_ids(trashed_nc)
    original_path = paths.get_original_path(trashed_nc)

    for dataset_id in dataset_ids:
        dataset = index.datasets.get(dataset_id)

        if dataset.is_archived:
            _LOG.debug("dataset.skip.archived", dataset_id=dataset.id)
            continue
        if original_path.as_uri() not in dataset.uris:
            _LOG.debug("dataset.skip.unknown_location", dataset_id=dataset.id)
            continue
        # There's something else in the location?
        if original_path.exists():
            _LOG.debug("dataset.skip.original_exists", dataset_id=dataset.id)
            continue

        # We've found an indexed, active dataset in the file, so restore.
        return original_path


if __name__ == '__main__':
    restore()
