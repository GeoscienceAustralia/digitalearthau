import collections
import click
import csv
from pathlib import Path
import structlog

from datacube import Datacube
from datacube.model import Dataset
from datacube.ui import click as ui
from digitalearthau import uiutil

_LOG = structlog.getLogger('dea-coherence')
_dataset_count, _siblings_count, _locationless_count, _archive_count = 0, 0, 0, 0
_downstream_dataset_count, _downstream_summary_prod_count = 0, 0
_bad_downstream_dataset = collections.deque({})

# Product list containing Telemetry, Level1, Level2, NBAR, PQ, PQ_Legacy, WOfS, FC products
# Level1_Scene datasets are expected not to have any locations (in future)
PRODUCT_TYPE_LIST = ['ls5_satellite_telemetry_data', 'ls5_level1_scene', 'ls5_nbar_scene', 'ls5_nbart_scene',
                     'ls5_pq_scene', 'ls5_pq_legacy_scene', 'ls5_nbar_albers', 'ls5_nbart_albers',
                     'ls5_pq_albers', 'ls5_fc_albers',
                     'ls7_satellite_telemetry_data', 'ls7_level1_scene', 'ls7_nbar_scene', 'ls7_nbart_scene',
                     'ls7_pq_scene', 'ls7_pq_legacy_scene', 'ls7_nbar_albers', 'ls7_nbart_albers',
                     'ls7_pq_albers', 'ls7_fc_albers',
                     'ls8_satellite_telemetry_data', 'ls8_level1_scene', 'ls8_level1_oli_scene', 'ls8_nbar_scene',
                     'ls8_nbart_scene', 'ls8_pq_scene', 'ls8_pq_legacy_scene', 'ls8_nbar_oli_scene',
                     'ls8_nbart_oli_scene', 'ls8_pq_oli_scene', 'ls8_nbar_albers', 'ls8_nbart_albers', 'ls8_pq_albers',
                     'ls8_nbar_oli_albers', 'ls8_nbart_oli_albers', 'ls8_fc_albers',
                     'wofs_albers']

# Ignore sibling check for the listed products
IGNORE_SIBLINGS = ['dsm1sv10']


@click.group(help=__doc__)
def cli():
    pass


@cli.command()
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
@click.option('--check-downstream-ds',
              is_flag=True,
              default=False,
              help='Check if downstream datasets have their ancestor datasets archived')
@click.option('--archive-siblings',
              is_flag=True,
              default=False,
              help="Archive sibling datasets (forces --check-ancestors)")
@click.option('--test-dc-config', '-C',
              default=None,
              help='Custom datacube config file (testing purpose only)')
@ui.parsed_search_expressions
def main(expressions, check_locationless, archive_locationless, check_ancestors, check_siblings, check_downstream_ds,
         archive_siblings, test_dc_config):
    """
    Find problem datasets using the index.

    - Datasets with no active locations (can be archived with --archive-locationless)
    - Datasets whose ancestors have been archived. (perhaps they've been reprocessed?)
    - Downstream datasets linked to locationless source datasets (datasets deleted from the disc and regenerated again
      for reprocessing)
           ex. derived albers files whose source dataset/s have been archived by sync tool
                 LS7_ETM_NBART_P54_GANBART01-002_112_082_20180910 (Archived)
                   |----- LS7_ETM_NBART_3577_-14_-35_20180910020329500000_v1537611829.nc (Active)
                   |----- LS7_ETM_NBART_3577_-14_-36_20180910020329500000_v1537611829.nc (Active)

    Intended to be used after running sync: when filesystem and index is consistent. Ideally
    you've synced the ancestor collections too.

    (the sync tool cannot do these checks "live", as we don't know how many locations there are of a dataset
    until the entire folder has been synced, and ideally the ancestors synced too.)
    TODO: This could be merged into it as a post-processing step, although it's less safe than sync if
    TODO: the index is being updated concurrently by another
    """
    global _dataset_count, _siblings_count, _locationless_count, _archive_count
    global _downstream_dataset_count, _downstream_summary_prod_count
    uiutil.init_logging()
    with Datacube(config=test_dc_config) as dc:
        _LOG.info('query', query=expressions)
        for dataset in dc.index.datasets.search(**expressions):
            _dataset_count += 1
            # Archive if it has no locations.
            # (the sync tool removes locations that don't exist anymore on disk,
            # but can't archive datasets as another path may be added later during the sync)
            if check_locationless or archive_locationless:
                if dataset.uris is not None and len(dataset.uris) == 0:
                    _locationless_count += 1

                    if archive_locationless:
                        dc.index.datasets.archive([dataset.id])
                        _archive_count += 1
                        _LOG.info("locationless." + str(dataset.type.name) + ".archived", dataset_id=str(dataset.id))
                    else:
                        _LOG.info("locationless." + str(dataset.type.name) + ".dry_run", dataset_id=str(dataset.id))

            # If an ancestor is archived, it may have been replaced. This one may need
            # to be reprocessed too.
            if check_ancestors or archive_siblings or check_siblings:
                _check_ancestors(check_siblings, archive_siblings, dc, dataset)

            # If downstream/derived datasets are linked to source datasets with archived locations, identify those
            # datasets and take appropriate action (archive downstream datasets)
            if check_downstream_ds:
                _manage_downstream_ds(dc, dataset)

        if check_downstream_ds:
            # Store the coherence result log in a csv file
            with open('bad_downstream_datasets.csv', 'w', newline='') as csvfile:
                _LOG.info("Coherence log is stored in a CSV file",
                          path=Path('bad_downstream_datasets.csv').absolute())
                writer = csv.writer(csvfile)
                writer.writerow(('Dataset_ID', 'Product_Type', 'Is_Active', 'Is_Archived', 'Format', 'Location'))

                for d in list(_bad_downstream_dataset):
                    writer.writerow((d.id, d.type, d.is_active, d.is_archived, d.format, d.uris))

        _LOG.info("coherence.finish",
                  datasets_count=_dataset_count,
                  locationless_count=_locationless_count,
                  siblings_count=_siblings_count,
                  archived_count=_archive_count,
                  bad_downstream_dataset_count=_downstream_dataset_count,
                  bad_downstream_summary_prod_count=_downstream_summary_prod_count)


def _archive_duplicate_siblings(dc, ids):
    """Archive old versions of duplicate datasets.

    When given a list of duplicate sibling datasets, keep the most recently
    indexed one and delete all the older ones.

    Return the number of archived duplicates.
    """
    # Look up the indexed time for each datset and store in a dict
    id_to_index_time = {ds_id: dc.index.datasets.get(ds_id).indexed_time
                        for ds_id in ids}

    # Sort by indexed time, and split into [newest : older_duplicates]
    newest_ds, *older_duplicates = collections.OrderedDict(sorted(id_to_index_time.items(),
                                                                  key=lambda t: t[1],
                                                                  reverse=True))

    dc.index.datasets.archive(older_duplicates)

    _LOG.info("dataset_id.archived", ids=older_duplicates)
    _LOG.info("dataset_id.kept", id=newest_ds)

    return len(older_duplicates)


def _check_ancestors(check_siblings: bool,
                     archive_siblings: bool,
                     dc: Datacube,
                     dataset: Dataset):
    global _siblings_count, _archive_count

    dataset = dc.index.datasets.get(dataset.id, include_sources=True)
    if dataset.sources:
        for classifier, source_dataset in dataset.sources.items():
            if source_dataset.is_archived:
                _LOG.info(
                    "ancestor." + str(dataset.type.name) + ".dry_run",
                    dataset_id=str(dataset.id),
                    source_type=classifier,
                    source_dataset_id=str(source_dataset.id)
                )
            elif (str(source_dataset.type.name) not in IGNORE_SIBLINGS) and (check_siblings or archive_siblings):
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
                    _LOG.info(str(dataset.type.name) + ".siblings.exists.dry_run",
                              dataset_id=str(dataset.id),
                              siblings=sibling_ids)

                    # Choose the most recent sibling and archive others
                    if archive_siblings:
                        _archive_count += _archive_duplicate_siblings(dc, sibling_ids + [str(dataset.id)])


def _manage_downstream_ds(dc: Datacube,
                          dataset: Dataset):
    """
    Need to manage the following two scenarios, with locationless source datasets:
       1) Identify and list all the downstream datasets (may include level2, NBAR, PQ, Albers, WOfS, FC) that are either
          Active or Archived.
       2) Identify and list all the downstream datasets (includes summary products) that are either Active or Archived.
    """

    # Fetch all the derived datasets using the source dataset.
    derived_dataset = _fetch_derived_datasets(dataset, dc)

    for dataset_list in derived_dataset:
        for d in dataset_list:
            if d.uris is None:
                # Append locationless source dataset to the list
                _bad_downstream_dataset.append(dataset)

                # Fetch all downstream datasets and append to the list
                _process_bad_derived_datasets(dataset, d, _fetch_derived_datasets(d, dc))


def _process_bad_derived_datasets(source_ds, dataset, derived_dataset):
    global _downstream_dataset_count, _downstream_summary_prod_count

    _LOG.info("locationless." + str(source_ds.type.name) + ".dry_run (Dataset ID: %s)" % str(source_ds.id),
              downstream_dataset_id=str(dataset.id),
              downstream_dataset_type=str(dataset.type.name),
              downstream_dataset_location=str(dataset.uris))
    _downstream_dataset_count += 1

    for dataset_list in derived_dataset:
        for d_datasets in dataset_list:
            # Exclude derived products such as summary products, FC_percentile products, etc.
            if d_datasets.type.name in PRODUCT_TYPE_LIST:
                _LOG.info("downstream." + str(dataset.type.name) + ".dry_run (Dataset ID: %s)" % str(dataset.id),
                          downstream_dataset_id=str(d_datasets.id),
                          downstream_dataset_type=str(d_datasets.type.name),
                          downstream_dataset_location=str(d_datasets.uris))
                _downstream_dataset_count += 1
            else:
                _LOG.info("derived." + str(dataset.type.name) + ".dry_run (Dataset ID: %s)" % str(dataset.id),
                          downstream_dataset_id=str(d_datasets.id),
                          dataset_type=str(d_datasets.type.name))
                _downstream_summary_prod_count += 1

            # Append all the downstream datasets derived from locationless source dataset to the list
            _bad_downstream_dataset.append(d_datasets)


def _fetch_derived_datasets(dataset, dc):
    to_process = {dataset.id}
    derived_dataset = collections.deque({})

    while to_process:
        derived = dc.index.datasets.get_derived(to_process.pop())
        to_process.update(d.id for d in derived)
        derived_dataset.append(derived)

    return derived_dataset


if __name__ == '__main__':
    main()
