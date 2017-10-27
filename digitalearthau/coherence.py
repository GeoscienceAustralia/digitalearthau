import click
import structlog

from datacube import Datacube
from datacube.model import Dataset
from datacube.ui import click as ui
from digitalearthau import uiutil

_LOG = structlog.getLogger('archive-locationless')


@click.command()
@click.option('--debug',
              is_flag=True,
              help='Enable debug logging')
@click.option('--check-locationless/--no-check-locationless',
              is_flag=True,
              default=True,
              help='Check any datasets without locations')
@click.option('--archive-locationless',
              is_flag=True,
              default=False,
              help="Archive datasets with no active locations (forces --check-locationless)")
@click.option('--check-ancestors',
              is_flag=True,
              help='Check if ancestor/source datasets are still active/valid')
@click.option('--check-siblings',
              is_flag=True,
              help='Check if ancestor datasets have other children of the same type (they may be duplicates)')
@ui.parsed_search_expressions
def main(expressions, archive_locationless, debug, check_locationless, check_ancestors, check_siblings):
    """
    Find problem datasets using the index.

    - Datasets with no active locations (can be archived with --archive-locationless)

    - Datasets whose ancestors have been archived. (perhaps they've been reprocessed?)

    Intended to be used after running sync: when filesystem and index is consistent. Ideally
    you've synced the ancestor collections too.

    (the sync tool cannot do these checks "live", as we don't know how many locations there are of a dataset
    until the entire folder has been synced, and ideally the ancestors synced too.
    TODO: This could be merged into it as a post-processing step, although it's less safe than sync if
    the index is being updated concurrently by another)
    """
    uiutil.init_logging()
    with Datacube() as dc:
        _LOG.info('query', query=expressions)
        count = 0
        archive_count = 0
        for dataset in dc.index.datasets.search(**expressions):
            count += 1
            # Archive if it has no locations.
            # (the sync tool removes locations that don't exist anymore on disk,
            # but can't archive datasets as another path may be added later during the sync)
            if check_locationless or archive_locationless:
                if dataset.uris is not None and len(dataset.uris) == 0:
                    _LOG.info("dataset.locationless", dataset_id=str(dataset.id))
                    if archive_locationless:
                        dc.index.datasets.archive([dataset.id])
                        archive_count += 1

            # If an ancestor is archived, it may have been replaced. This one may need
            # to be reprocessed too.
            if check_ancestors:
                _check_ancestors(check_siblings, dc, dataset)

        _LOG.info("coherence.finish", count=count, archive_count=archive_count)


def _check_ancestors(check_siblings: bool, dc: Datacube, dataset: Dataset):
    dataset = dc.index.datasets.get(dataset.id, include_sources=True)
    if dataset.sources:
        for classifier, source_dataset in dataset.sources.items():
            if source_dataset.is_archived:
                _LOG.info(
                    "dataset.ancestor_defunct",
                    dataset_id=str(dataset.id),
                    source_type=classifier,
                    source_dataset_id=str(source_dataset.id)
                )
            elif check_siblings:
                # If a source dataset has other siblings they may be duplicates.
                # (this only applies to source products that are 1:1 with
                # descendants, not pass-to-scene or scene-to-tile conversions)
                siblings = dc.index.datasets.get_derived(source_dataset.id)
                # Only active siblings of the same type.
                siblings = [
                    s for s in siblings
                    if s != dataset and not s.is_archived and s.type == dataset.type
                ]
                if siblings:
                    _LOG.info("dataset.siblings_exist",
                              dataset_id=str(dataset.id),
                              siblings=[str(d.id) for d in siblings])
                    # TODO Archive? This is currently purely for reporting, but if we know that a duplicate
                    #      sibling exists which is better or newer, we could possibly archive this one now.
                    # (deciding whether "duplicate", "better" and "newer" are all quite nuanced, though)


if __name__ == '__main__':
    main()
