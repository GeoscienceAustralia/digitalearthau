"""

TODO:
- [ ] Logging
- [ ] Error Handling

"""

# pylint: disable=map-builtin-not-iterating

import sys
from pathlib import Path
from typing import Sequence, NamedTuple, Generator

import yaml

import datacube
from datacube.model import Dataset
from datacube.model.utils import make_dataset
from datacube.virtual import construct
from ._dask import dask_compute_stream
from .file_utils import dataset_to_geotif_yaml, calc_uris, _get_filename


class NonVPTask(NamedTuple):
    dataset: Dataset
    measurements: Sequence[str]
    transform: str
    file_output: str


class Dataset2Dataset:
    def __init__(self, *, config=None, config_file=None, dc_env=None):
        if config is not None:
            self.config = config
        else:
            self.config = config = yaml.safe_load(Path(config_file).read_bytes())

        # Connect to the ODC Index
        self.dc = datacube.Datacube(env=dc_env)
        self.input_product_name = self.config['specification']['input_product']
        self.input_product = self.dc.index.products.get_by_name(self.input_product_name)

    def generate_tasks(self, limit=3) -> Generator[NonVPTask]:
        # Find which datasets needs to be processed
        datasets = self.dc.index.datasets.search(limit=limit, product=self.input_product_name)

        tasks = (self.generate_task(ds) for ds in datasets)

        return tasks

    def generate_task(self, dataset) -> NonVPTask:
        return NonVPTask(dataset,
                         self.config['specification']['measurements'],
                         self.config['specification']['transform'],
                         'file_output')  # TODO

    def execute_with_dask(self, tasks: Sequence[NonVPTask]):
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

    def execute_task(self, task: NonVPTask):
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
