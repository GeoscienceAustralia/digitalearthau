"""

TODO:
- [ ] Logging
- [ ] Error Handling

"""

import sys
from collections import namedtuple

import click
import yaml

import datacube
from datacube.model.utils import make_dataset
from datacube.virtual import construct
from datacube.virtual.impl import VirtualDatasetBag
from ._dask import dask_compute_stream
from .file_utils import dataset_to_geotif_yaml, calc_uris, _get_filename

Task = namedtuple('Task',
                  ['box',  # A Virtual Product Box containing source datasets
                   'virtual_product_def',  # A virtual product definition for processing the boxes
                   'file_output',  # A dict with `location` and `file_path_template`
                   'output_product',  # A datacube.model.DatasetType of the destination product
                   ])


class D4:
    def __init__(self, config_file):
        with open(config_file) as conf_file:
            self.config = config = yaml.safe_load(conf_file)
        self.vproduct = construct(**config['virtual_product_specification'])

        # Connect to the ODC Index
        dc = datacube.Datacube()
        self.input_product_name = self.config['task_generation']['input_product']
        self.input_product = dc.index.products.get_by_name(self.input_product_name)
        self.output_product = dc.index.products.get_by_name(config['task_generation']['output_product'])

    def generate_tasks(self):
        # Find which datasets needs to be processed
        datasets = dc.index.datasets.search(limit=3, product=self.config['task_generation']['input_product'])
        # datasets = datasets_that_need_to_be_processed(dc.index, config['task_generation']['input_product'],
        #                                               output_product_name)

        # Divide into a sequence of tasks
        bags = (
            VirtualDatasetBag([dataset], None, {input_product_name: self.input_product})
            for dataset in datasets
        )

        boxes = (self.vproduct.group(bag) for bag in bags)

        tasks = map(task_maker(self.config, self.output_product), boxes)

        tasks = list(tasks)
        print(len(tasks))
        print(tasks)
        return tasks

    def execute_with_dask(self, tasks):
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
            pass


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


@click.command()
@click.argument('config_file')
def main(config_file):
    # Load Configuration file
    d4 = D4(config_file)

    tasks = d4.generate_tasks()

    d4.execute_with_dask(tasks)


def task_maker(config, output_product):
    def make_task(box):
        return Task(
            box=box,
            virtual_product_def=config['virtual_product_specification'],
            file_output=config['file_output'],
            output_product=output_product,
        )

    return make_task


if __name__ == '__main__':
    main()
