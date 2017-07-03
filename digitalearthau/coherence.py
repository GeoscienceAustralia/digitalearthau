import click
import structlog

from datacube import Datacube
from datacube.model import Dataset
from datacube.ui import click as ui
from digitalearthau import uiutil

_LOG = structlog.getLogger('archive-locationless')


@click.command(help="Scan the index to archive duplicates and defunct/replaced datasets. "
                    "Intended to be used after running sync: when filesystem and index is consistent")
@click.option('--debug',
              is_flag=True,
              help='Enable debug logging')
@click.option('--check-locationless/--no-check-locationless',
              is_flag=True,
              default=True,
              help='Check/archive any datasets without locations')
@click.option('--check-ancestors',
              is_flag=True,
              help='Check if ancestor/source datasets are still active/valid')
@click.option('--check-siblings',
              is_flag=True,
              help='Check if ancestor datasets have other children of the same type (they may be duplicates)')
@click.option('--dry-run',
              is_flag=True,
              help="Don't make any changes (ie. don't archive anything)")
@ui.parsed_search_expressions
def main(expressions, dry_run, debug, check_locationless, check_ancestors, check_siblings):
    with Datacube() as dc:
        _LOG.info('query', query=expressions)
        count = 0
        archive_count = 0
        for dataset in dc.index.datasets.search(**expressions):
            count += 1
            # Archive if it has no locations.
            # (the sync tool removes locations that don't exist anymore on disk,
            # but can't archive datasets as another path may be added later during the sync)
            if check_locationless:
                if dataset.uris is not None and len(dataset.uris) == 0:
                    _LOG.info("dataset.locationless", dataset_id=str(dataset.id))
                    if not dry_run:
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
