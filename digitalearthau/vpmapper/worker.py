"""

TODO:
- [ ] Logging
- [ ] Error Handling

"""

import sys
from collections import namedtuple

import click
import yaml
from pathlib import Path

import datacube
from datacube.model.utils import make_dataset
from datacube.virtual import construct
from datacube.virtual.impl import VirtualDatasetBag
from .file_utils import dataset_to_geotif_yaml, calc_uris, _get_filename
from ._dask import dask_compute_stream

Task = namedtuple('Task',
                  ['box',                   # A Virtual Product Box containing source datasets
                   'virtual_product_def',   # A virtual product definition for processing the boxes
                   'file_output',           # A dict with `location` and `file_path_template`
                   'output_product',        # A datacube.model.DatasetType of the destination product
                   ])


@click.command()
@click.argument('config_file')
def main(config_file):
    # Load Configuration file
    with open(config_file) as conf_file:
        config = yaml.safe_load(conf_file)
    vproduct = construct(**config['virtual_product_specification'])

    # Connect to the ODC Index
    dc = datacube.Datacube()
    input_product_name = config['task_generation']['input_product']
    input_product = dc.index.products.get_by_name(input_product_name)
    output_product = dc.index.products.get_by_name(config['task_generation']['output_product'])

    # Find which datasets needs to be processed
    datasets = dc.index.datasets.search(limit=3, product=config['task_generation']['input_product'])
    # datasets = datasets_that_need_to_be_processed(dc.index, config['task_generation']['input_product'],
    #                                               output_product_name)

    # Divide into a sequence of tasks
    bags = (
        VirtualDatasetBag([dataset], None, {input_product_name: input_product})
        for dataset in datasets
    )

    boxes = (vproduct.group(bag) for bag in bags)

    tasks = map(task_maker(config, output_product), boxes)

    tasks = list(tasks)
    print(len(tasks))
    print(tasks)

    # Execute the tasks across the dask cluster
    from dask.distributed import Client
    client = Client()
    completed = dask_compute_stream(client, execute_task, tasks)

    for result in completed:
        try:
            print(result)
        except Exception as e:
            print(e)
            print(sys.exc_info()[0])


def task_maker(config, output_product):
    def make_task(box):
        return Task(
            box=box,
            virtual_product_def=config['virtual_product_specification'],
            file_output=config['file_output'],
            output_product=output_product,
        )

    return make_task


def execute_task(task: Task):
    vproduct = construct(**task.virtual_product_def)

    # Load and perform processing
    output_data = vproduct.fetch(task.box)

    input_dataset = next(iter(task.box.pile))

    # compute base filename
    variable_params = {band: None
                       for band in vproduct.output_measurements(task.box.product_definitions)}
    base_filename = _get_filename(task.file_output, sources=input_dataset.item()[0])

    # generate dataset metadata
    uri, band_uris = calc_uris(base_filename, variable_params)
    odc_dataset = make_dataset(product=task.output_product,
                               sources=input_dataset.item(),
                               extent=task.box.geobox.extent,
                               center_time=input_dataset.time.item(),
                               uri=uri,
                               band_uris=band_uris,
                               app_info=task.virtual_product_def,
                               )

    # write data to disk
    dataset_to_geotif_yaml(
        dataset=output_data,
        odc_dataset=odc_dataset,
        filename=base_filename,
        variable_params=variable_params,
    )

    # record dataset record to database
    # OR NOT
    return base_filename


if __name__ == '__main__':
    main()
