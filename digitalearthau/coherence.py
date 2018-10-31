import click
import collections
import csv
import structlog

from datacube import Datacube
from datacube.model import Dataset
from datacube.ui import click as ui
from datetime import datetime as dt
from digitalearthau import uiutil, collections
from pathlib import Path

_LOG = structlog.getLogger('dea-coherence')
_dataset_cnt, _ancestor_cnt, _locationless_cnt, _archive_locationless_cnt = 0, 0, 0, 0
_downstream_dataset_cnt = 0
_product_type_list = []

# Product list containing Level2_Scenes, NBAR, PQ, PQ_Legacy, WOfS, FC products
IGNORE_PRODUCT_TYPE_LIST = ['telemetry', 'ls8_level1_scene', 'ls7_level1_scene',
                            'ls5_level1_scene', 'pq_count_summary',
                            'pq_count_annual_summary']

DATE_NOW = dt.now().strftime('%Y-%m-%d')
TIME_NOW = dt.now().strftime('%H-%M-%S')

DEFAULT_CSV_PATH = '/g/data/v10/work/coherence/{0}/erroneous_datasets_{1}.csv'.format(DATE_NOW, TIME_NOW)


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
@click.option('--check-downstream',
              is_flag=True,
              default=False,
              help='Check if archived parent or locationless parent have children (downstream)')
@click.option('--test-dc-config', '-C',
              default=None,
              help='Custom datacube config file (testing purpose only)')
@ui.parsed_search_expressions
def main(expressions, check_locationless, archive_locationless, check_ancestors,
         check_downstream, test_dc_config):
    """
    Find problem datasets using the index.

    - Datasets with no active locations (can be archived with --archive-locationless)
    - Datasets whose ancestors have been archived. (perhaps they've been reprocessed?)
    - Downstream datasets linked to locationless source datasets (datasets deleted from the disc and
      regenerated again for reprocessing)
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
    global _dataset_cnt, _ancestor_cnt, _locationless_cnt, _archive_locationless_cnt
    global _downstream_dataset_cnt, _product_type_list
    uiutil.init_logging()

    # Write the header to the CSV file
    with open(DEFAULT_CSV_PATH, 'w', newline='') as csvfile:
        _LOG.info("Coherence log is stored in a CSV file",
                  path=Path(DEFAULT_CSV_PATH).absolute())
        writer = csv.writer(csvfile)
        writer.writerow(('Category', 'Dataset_Type', 'Dataset_ID', 'Is_Dataset_Archived',
                         'Parent_Type', 'Parent_ID', 'Is_Parent_Archived',
                         'Dataset_Location'))

    with Datacube(config=test_dc_config) as dc:
        collections.init_nci_collections(dc.index)
        _product_type_list = collections.registered_collection_names()

        # collections.registered_collection_names is not fetching nbart_scenes. Hence add them to the list
        _product_type_list.extend(['ls5_nbart_scene', 'ls7_nbart_scene', 'ls8_nbart_scene'])

        _LOG.info('query', query=expressions)
        for dataset in dc.index.datasets.search(**expressions):
            _dataset_cnt += 1
            # Archive if it has no locations.
            # (the sync tool removes locations that don't exist anymore on disk,
            # but can't archive datasets as another path may be added later during the sync)
            if check_locationless or archive_locationless:
                _product_type_list = [x for x in _product_type_list if x not in IGNORE_PRODUCT_TYPE_LIST]

                # Level1 scenes are expected not to have location
                if len(dataset.uris) == 0 and str(dataset.type.name) in _product_type_list:
                    _locationless_cnt += 1

                    if archive_locationless:
                        dc.index.datasets.archive([dataset.id])
                        _archive_locationless_cnt += 1
                        _LOG.info(f"locationless.{dataset.type.name}.archived", dataset_id=str(dataset.id))
                    else:
                        _LOG.info(f"locationless.{dataset.type.name}", dataset_id=str(dataset.id))

                    # Log derived product with locationless datasets, to the CSV file
                    _log_to_csvfile(f"locationless.{dataset.type.name}", dataset)

            # Check for ancestors
            if check_ancestors:
                _check_ancestors(dc, dataset)

            # If downstream/derived datasets are linked to archived parent or locationless parent, identify
            # those datasets and take appropriate action (archive downstream datasets)
            # TODO: For now, only list downstream datasets.
            if check_downstream:
                _manage_downstream_ds(dc, dataset)

        _LOG.info("coherence.finish",
                  datasets_count=_dataset_cnt,
                  archived_ancestor_count=_ancestor_cnt,
                  locationless_count=_locationless_cnt,
                  archived_locationless_cnt=_archive_locationless_cnt,
                  downstream_dataset_error_count=_downstream_dataset_cnt)


def _check_ancestors(dc: Datacube,
                     dataset: Dataset):
    global _ancestor_cnt

    dataset = dc.index.datasets.get(dataset.id, include_sources=True)
    if dataset.sources:
        for classifier, source_dataset in dataset.sources.items():
            if source_dataset.is_archived:
                _LOG.info(
                    f"archived.parent.{source_dataset.type.name}.derived.{dataset.type.name}",
                    dataset_id=str(dataset.id),
                    archived_parent_type=str(source_dataset.type.name),
                    archived_parent_id=str(source_dataset.id)
                )
                _ancestor_cnt += 1

                # Log ancestor datasets to the CSV file
                _log_to_csvfile(f"archived.parent.{source_dataset.type.name}.derived.{dataset.type.name}",
                                dataset,
                                source_dataset)


def _manage_downstream_ds(dc: Datacube,
                          dataset: Dataset):
    """
    Need to manage the following two scenarios, with locationless source datasets:
       1) Identify and list all the downstream datasets (may include level2, NBAR, PQ, Albers, WOfS, FC)
          that are either Active or Archived.
       2) Identify and list all the downstream datasets (includes summary products) that are either
          Active or Archived.
    """
    global _product_type_list

    # Fetch all the derived datasets using the source dataset.
    derived_datasets = _fetch_derived_datasets(dataset, dc)

    for d in derived_datasets:
        # Find locationless datasets and they are not telemetry/level1 scenes.
        if len(d.uris) == 0 and str(d.type.name) in _product_type_list:
            # Fetch all downstream datasets associated with locationless parent
            # and append to the list
            _process_derived_datasets(dataset, d, _fetch_derived_datasets(d, dc))


def _process_derived_datasets(source_ds, dataset, derived_datasets):
    global _downstream_dataset_cnt, _product_type_list
    _downstream_dataset_cnt += 1

    # Log locationless parent or ancestor parent to the CSV file
    if dataset.is_archived:
        _parent = "archived"
    else:
        _parent = "locationless"

    _LOG.info(f"{_parent}.{dataset.type.name}",
              downstream_dataset_id=str(dataset.id),
              downstream_dataset_type=str(dataset.type.name),
              downstream_dataset_location=str(dataset.uris))

    _log_to_csvfile(f"{_parent}.{dataset.type.name}", dataset, source_ds)

    for d_dataset in derived_datasets:
        # Exclude derived products such as summary products, FC_percentile products, etc.
        if d_dataset.type.name in _product_type_list:
            _LOG.info(f"{_parent}.parent.{dataset.type.name}.derived.{d_dataset.type.name}",
                      downstream_dataset_id=str(d_dataset.id),
                      downstream_dataset_type=str(d_dataset.type.name),
                      downstream_dataset_location=str(d_dataset.uris))
            _downstream_dataset_cnt += 1

            # Log downstream datasets linked to locationless source datasets, to the CSV file
            _log_to_csvfile(
                f"{_parent}.parent.{dataset.type.name}.derived.{d_dataset.type.name}",
                d_dataset,
                dataset)


def _fetch_derived_datasets(dataset, dc):
    to_process = {dataset.id}
    derived_datasets = set()

    while to_process:
        derived = dc.index.datasets.get_derived(to_process.pop())
        to_process.update(d.id for d in derived)
        derived_datasets.update(derived)

    return derived_datasets


def _log_to_csvfile(click_options, d, parent=None):
    # Store the coherence result log in a csv file
    with open(DEFAULT_CSV_PATH, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow((click_options, d.type.name, d.id, d.is_archived,
                         parent.type.name, parent.id, parent.is_archived, d.uris))


if __name__ == '__main__':
    main()
