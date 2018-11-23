import collections
import csv
from datetime import datetime as dt
from pathlib import Path

import click
import structlog

from datacube import Datacube
from datacube.model import Dataset
from datacube.ui import click as ui
from digitalearthau import uiutil, collections, paths

_LOG = structlog.getLogger('dea-coherence')
DATASET_CNT, ANCESTOR_CNT, LOCATIONLESS_CNT, ARCHIVED_LOCATIONLESS_CNT = 0, 0, 0, 0
DOWNSTREAM_DS_CNT = 0

# Product list containing Level2_Scenes, NBAR, PQ, PQ_Legacy, WOfS, FC products
IGNORED_PRODUCTS_LIST = ['telemetry', 'ls8_level1_scene', 'ls7_level1_scene',
                         'ls5_level1_scene', 'pq_count_summary',
                         'pq_count_annual_summary']

NOW = dt.now()
CSV_OUTPUT_FILE = paths.NCI_WORK_ROOT / f"coherence/{NOW:%Y-%m-%d}/erroneous_datasets_{NOW:%H-%M-%S}.csv"


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
              help='Check if the dataset has an archived parent dataset')
@click.option('--check-downstream',
              is_flag=True,
              default=False,
              help='Check if an archived or locationless dataset has children (forces --check-locationless)')
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
    - Datasets whose ancestors are locationless
    - Downstream datasets linked to locationless source datasets (datasets deleted from the disc and
      regenerated again for reprocessing)

    ::

       eg. derived albers files whose source dataset/s have been archived by sync tool
             LS7_ETM_NBART_P54_GANBART01-002_112_082_20180910 (Archived)
               |----- LS7_ETM_NBART_3577_-14_-35_20180910020329500000_v1537611829.nc (Active)
               |----- LS7_ETM_NBART_3577_-14_-36_20180910020329500000_v1537611829.nc (Active)

    Intended to be used after running sync: when filesystem and index is consistent. Ideally
    you've synced the ancestor collections too.

    (the sync tool cannot do these checks "live", as we don't know how many locations there are of a dataset
    until the entire folder has been synced, and ideally the ancestors synced too.)
    """
    global DATASET_CNT, LOCATIONLESS_CNT, ARCHIVED_LOCATIONLESS_CNT
    uiutil.init_logging()

    check_locationless = check_locationless or archive_locationless or check_downstream

    collections.init_nci_collections(None)
    products_to_check = set(product
                            for product in collections.registered_collection_names()
                            if product not in IGNORED_PRODUCTS_LIST)

    _create_output_csv()

    with Datacube(config=test_dc_config) as dc:
        _LOG.info('query', query=expressions)

        for dataset in dc.index.datasets.search(**expressions):
            DATASET_CNT += 1

            # Some datasets are not expected to have a location and should be skipped (eg, level1, telemetry)
            if (dataset.type.name in products_to_check) and check_locationless:
                if len(dataset.uris) == 0:
                    LOCATIONLESS_CNT += 1
                    ds_state = "locationless"

                    if archive_locationless:
                        # Archive if it has no locations.
                        # (the sync tool removes locations that don't exist anymore on disk,
                        # but can't archive datasets as another path may be added later during the sync)
                        dc.index.datasets.archive([dataset.id])
                        ARCHIVED_LOCATIONLESS_CNT += 1
                        ds_state = "archived"
                    elif dataset.is_archived:
                        ds_state = "archived"

                    # Log locationless parent or ancestor parent to the CSV file
                    _LOG.info(f"{ds_state}.{dataset.type.name}",
                              dataset_id=str(dataset.id),
                              dataset_location=str(dataset.uris))

                    _log_to_csvfile(f"{ds_state}.{dataset.type.name}", dataset)

                    # Find any datasets derived from this one and log them as problematic
                    if check_downstream:
                        _record_defunct_descendant_dataset(dc, dataset, ds_state)

            # Check for archived ancestors
            if check_ancestors:
                _record_archived_ancestors(dc, dataset)

    _LOG.info("coherence.finish",
              datasets_count=DATASET_CNT,
              archived_ancestor_count=ANCESTOR_CNT,
              locationless_count=LOCATIONLESS_CNT,
              archived_locationless_count=ARCHIVED_LOCATIONLESS_CNT,
              downstream_dataset_error_count=DOWNSTREAM_DS_CNT)


def _create_output_csv():
    """Create the output CSV with header."""
    CSV_OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CSV_OUTPUT_FILE, 'w', newline='') as csvfile:
        _LOG.info("Coherence log is stored in a CSV file",
                  path=Path(CSV_OUTPUT_FILE).absolute())
        writer = csv.writer(csvfile)
        writer.writerow(('Category', 'Dataset_Type', 'Dataset_ID', 'Is_Dataset_Archived',
                         'Parent_Type', 'Parent_ID', 'Is_Parent_Archived',
                         'Dataset_Location'))


def _record_archived_ancestors(dc: Datacube,
                               dataset: Dataset):
    global ANCESTOR_CNT
    dataset = dc.index.datasets.get(dataset.id, include_sources=True)
    if dataset.sources:
        for source_dataset in dataset.sources.values():
            if source_dataset.is_archived:
                _LOG.info(
                    f"archived.parent.{source_dataset.type.name}.derived.{dataset.type.name}",
                    dataset_id=str(dataset.id),
                    archived_parent_type=str(source_dataset.type.name),
                    archived_parent_id=str(source_dataset.id)
                )
                ANCESTOR_CNT += 1

                # Log ancestor datasets to the CSV file
                _log_to_csvfile(f"archived.parent.{source_dataset.type.name}.derived.{dataset.type.name}",
                                dataset,
                                source_dataset)


def _record_defunct_descendant_dataset(datacube, dataset, derived_ds_problem):
    """
    Identify and list all the downstream datasets (may include level2, NBAR, PQ, Albers, WOfS, FC)
    that linked to active locationless parent or archived parent.
    """
    global DOWNSTREAM_DS_CNT
    # Fetch all the derived datasets using the locationless parent or ancestor dataset.
    fetch_descendant_datasets = _fetch_derived_datasets(datacube, dataset)

    for d_dataset in fetch_descendant_datasets:
        _LOG.info(f"{derived_ds_problem}.parent.{dataset.type.name}.derived.{d_dataset.type.name}",
                  descendant_ds_id=str(d_dataset.id),
                  descendant_ds_type=str(d_dataset.type.name),
                  descendant_ds_location=str(d_dataset.uris))

        # Log downstream datasets linked to locationless source datasets, to the CSV file
        _log_to_csvfile(
            f"{derived_ds_problem}.parent.{dataset.type.name}.derived.{d_dataset.type.name}",
            d_dataset,
            dataset)

        DOWNSTREAM_DS_CNT += 1


def _fetch_derived_datasets(dc, dataset):
    to_process = {dataset.id}
    derived_datasets = set()

    while to_process:
        derived = dc.index.datasets.get_derived(to_process.pop())
        to_process.update(d.id for d in derived)
        derived_datasets.update(derived)

    return derived_datasets


def _log_to_csvfile(category, dataset, parent=None):
    # Store the coherence result log in a csv file
    with open(CSV_OUTPUT_FILE, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        if parent:
            writer.writerow((category,
                             dataset.type.name,
                             dataset.id,
                             dataset.is_archived,
                             parent.type.name,
                             parent.id,
                             parent.is_archived,
                             dataset.uris))
        else:
            writer.writerow((category,
                             dataset.type.name,
                             dataset.id,
                             dataset.is_archived,
                             None,
                             None,
                             None,
                             dataset.uris))


if __name__ == '__main__':
    main()
