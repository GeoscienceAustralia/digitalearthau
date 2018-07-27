import click
import structlog

from datacube import Datacube
from datacube.model import Dataset
from datacube.ui import click as ui
from digitalearthau import uiutil

_LOG = structlog.getLogger('archive-locationless')
_siblings_count = 0


@click.command()
@click.option('--check-locationless/--no-check-locationless',
              is_flag=True,
              default=False,
              help='Check any datasets without locations')
@click.option('--archive-locationless',
              is_flag=True,
              default=False,
              help="Archive datasets with no active locations (forces --check-locationless)")
@click.option('--check-ancestors',
              is_flag=True,
              default=False,
              help='Check if ancestor/source datasets are still active/valid')
@click.option('--check-siblings',
              is_flag=True,
              default=False,
              help='Check if ancestor datasets have other children of the same type (they may be duplicates)')
@click.option('--archive-siblings',
              is_flag=True,
              default=False,
              help="Archive sibling datasets (forces --check-ancestors)")
@click.option('--test-dc-config', '-C',
              help='Custom datacube config file (testing purpose only)')
@ui.parsed_search_expressions
def main(expressions, check_locationless, archive_locationless, check_ancestors, check_siblings, archive_siblings,
         test_dc_config):
    """
    Find problem datasets using the index.

    - Datasets with no active locations (can be archived with --archive-locationless)

    - Datasets whose ancestors have been archived. (perhaps they've been reprocessed?)

    Intended to be used after running sync: when filesystem and index is consistent. Ideally
    you've synced the ancestor collections too.

    (the sync tool cannot do these checks "live", as we don't know how many locations there are of a dataset
    until the entire folder has been synced, and ideally the ancestors synced too.)
    TODO: This could be merged into it as a post-processing step, although it's less safe than sync if
    TODO: the index is being updated concurrently by another
    """
    global _siblings_count
    uiutil.init_logging()
    config_file = test_dc_config if test_dc_config else ''
    with Datacube(config=config_file) as dc:
        _LOG.info('query', query=expressions)
        count = 0
        archive_count = 0
        locationless_count = 0
        for dataset in dc.index.datasets.search(**expressions):
            count += 1
            # Archive if it has no locations.
            # (the sync tool removes locations that don't exist anymore on disk,
            # but can't archive datasets as another path may be added later during the sync)
            if check_locationless or archive_locationless:
                if dataset.uris is not None and len(dataset.uris) == 0:
                    locationless_count += 1
                    
                    if archive_locationless:
                        dc.index.datasets.archive([dataset.id])
                        archive_count += 1
                        _LOG.info("locationless_dataset_id.archived", dataset_id=str(dataset.id))
                    else:
                        _LOG.info("locationless_dataset_id", dataset_id=str(dataset.id))
            
            # If an ancestor is archived, it may have been replaced. This one may need
            # to be reprocessed too.
            if check_ancestors or archive_siblings or check_siblings:
                archive_count += _check_ancestors(check_ancestors, check_siblings, archive_siblings, dc, dataset)
        
        _LOG.info("coherence.finish",
                  datasets_count=count,
                  locationless_count=locationless_count,
                  siblings_count=_siblings_count,
                  archived_count=archive_count)


def _archive_dataset_ids(dc, ds_id, sibling_ids):
    # sibling/s indexed_time dictionary
    sb_itime_dict = dict()
    
    # Count of dataset id's archived
    archive_count = 0
    
    # dataset indexed time
    ds_itime = dc.index.datasets.get(ds_id).indexed_time
    for key, s_id in enumerate(sibling_ids):
        sb_itime_dict[s_id] = dc.index.datasets.get(s_id).indexed_time
    
    # new dataset id to retain
    new_id = ds_id
    for id, itime in sb_itime_dict.items():
        # Check if dataset indexed time is less than or equal to sibling dataset indexed time
        if ds_itime <= itime:
            ds_id_archive = new_id
            ds_archive_time = ds_itime
            
            # Latest sibling dataset id to retain
            new_id = id
            ds_itime = itime
        else:
            ds_id_archive = id
            ds_archive_time = itime
        
        dc.index.datasets.archive([ds_id_archive])
        _LOG.info("\tarchived_dataset_id", id=str(ds_id_archive), indexed_time=str(ds_archive_time))
        archive_count += 1
    
    _LOG.info("\trecent_dataset_id", id=str(new_id), indexed_time=str(ds_itime))
    return archive_count


def _check_ancestors(check_ancestors: bool,
                     check_siblings: bool,
                     archive_siblings: bool,
                     dc: Datacube,
                     dataset: Dataset):
    global _siblings_count
    ancestors_archive_count = 0
    dataset = dc.index.datasets.get(dataset.id, include_sources=True)
    if dataset.sources:
        for classifier, source_dataset in dataset.sources.items():
            if check_ancestors and source_dataset.is_archived:
                _LOG.info(
                    "ancestor.dataset_id",
                    dataset_id=str(dataset.id),
                    source_type=classifier,
                    source_dataset_id=str(source_dataset.id)
                )
            elif check_siblings or archive_siblings:
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
                    _siblings_count += 1
                    sibling_ids = [str(d.id) for d in siblings]
                    _LOG.info("dataset.siblings_exist",
                              dataset_id=str(dataset.id),
                              siblings=sibling_ids)
                    
                    # Choose the most recent sibling and archive others
                    if archive_siblings:
                        ancestors_archive_count += _archive_dataset_ids(dc, dataset.id, sibling_ids)
    
    return ancestors_archive_count


if __name__ == '__main__':
    main()