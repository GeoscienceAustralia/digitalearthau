"""

TODO:
- [ ] Logging
- [ ] Error Handling

"""

# pylint: disable=map-builtin-not-iterating

import sys
from collections import namedtuple
from typing import Sequence

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
    def __init__(self, *, config=None, config_file=None, dc_env=None):
        if config is not None:
            self.config = config
        else:
            self.config = config = yaml.safe_load(config_file)
        self.vproduct = construct(**config['virtual_product_specification'])

        # Connect to the ODC Index
        self.dc = datacube.Datacube(env=dc_env)
        self.input_product_name = self.config['task_generation']['input_product']
        self.input_product = self.dc.index.products.get_by_name(self.input_product_name)
        self.output_product = self.dc.index.products.get_by_name(config['task_generation']['output_product'])

    def generate_tasks(self, limit=3) -> Sequence[Task]:
        # Find which datasets needs to be processed
        datasets = self.dc.index.datasets.search(limit=limit, product=self.config['task_generation']['input_product'])

        tasks = (self.generate_task(ds) for ds in datasets)

        return tasks

    def generate_task(self, dataset) -> Task:
        bag = VirtualDatasetBag([dataset], None, {dataset.type.name: dataset.type})
        box = self.vproduct.group(bag)
        task = self._make_task(box)
        return task

    def _make_task(self, box) -> Task:
        return Task(
            box=box,
            virtual_product_def=self.config['virtual_product_specification'],
            file_output=self.config['file_output'],
            output_product=self.output_product,
        )

    def execute_with_dask(self, tasks: Sequence[Task]):
        # Execute the tasks across the dask cluster
        from dask.distributed import Client
        client = Client()
        completed = dask_compute_stream(client, self.execute_task, tasks)

        for result in completed:
            try:
                print(result)
            except Exception as e:
                print(e)
                print(sys.exc_info()[0])
            pass

    def execute_task(self, task: Task):
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

        return base_filename
