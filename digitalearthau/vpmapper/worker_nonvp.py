"""

TODO:
- [ ] Logging
- [ ] Error Handling

"""

# pylint: disable=map-builtin-not-iterating
import importlib
import sys
from pathlib import Path
from typing import Sequence, NamedTuple, Iterable, Mapping

import structlog
import yaml

import datacube
from datacube.model import Dataset
from datacube.testutils.io import native_load
from datacube.virtual import Transformation
from ._dask import dask_compute_stream
from .file_utils import dataset_to_geotif_yaml, calc_uris, _get_filename

_LOG = structlog.get_logger()


class NonVPTask(NamedTuple):
    dataset: Dataset
    measurements: Sequence[str]
    renames: Mapping[str, str]
    transform: str
    filename_template: str


class Dataset2Dataset:
    def __init__(self, *, config=None, config_file=None, dc_env=None):
        if config is not None:
            self.config = config
        else:
            self.config = config = yaml.safe_load(Path(config_file).read_bytes())

        # Connect to the ODC Index
        self.dc = datacube.Datacube(env=dc_env)
        self.input_product_name = self.config['specification']['product']
        self.input_product = self.dc.index.products.get_by_name(self.input_product_name)

    def generate_tasks(self, limit=3) -> Iterable[NonVPTask]:
        # Find which datasets needs to be processed
        datasets = self.dc.index.datasets.search(limit=limit, product=self.input_product_name)

        tasks = (self.generate_task(ds) for ds in datasets)

        return tasks

    def generate_task(self, dataset) -> NonVPTask:
        return NonVPTask(dataset=dataset,
                         measurements=self.config['specification']['measurements'],
                         renames=self.config['specification']['measurement_renames'],
                         transform=self.config['specification']['transform'],
                         filename_template=self.config['file_output'])


def execute_with_dask(tasks: Iterable[NonVPTask]):
    # Execute the tasks across the dask cluster
    from dask.distributed import Client
    client = Client()
    _LOG.info('started dask', dask_client=client)
    completed = dask_compute_stream(client,
                                    execute_task,
                                    tasks)
    _LOG.info('processing task stream')
    for result in completed:
        try:
            print(result)
        except Exception as e:
            print(e)
            print(sys.exc_info()[0])
        pass
    _LOG.info('completed')


def execute_task(task: NonVPTask):
    log = _LOG.bind(task=task)
    transform = _import_transform(task.transform)

    # compute base filename
    variable_params = {name: measurement
                       for name, measurement in transform.measurements({}).items()}
    base_filename = _get_filename(task.filename_template, input_dataset=task.dataset)

    # Load and process data
    data = native_load(task.dataset, measurements=task.measurements, dask_chunks={'x': 1000, 'y': 1000})
    data = data.rename(task.renames)

    log.info('data loaded')

    output_data = transform.compute(data)

    log.info('processed transform', output_data=output_data)

    output_data = output_data.compute()
    # generate dataset metadata
    uri, band_uris = calc_uris(base_filename, variable_params)
    # odc_dataset = make_dataset(product=task.output_product,
    #                            sources=input_dataset.item(),
    #                            extent=task.box.geobox.extent,
    #                            center_time=input_dataset.time.item(),
    #                            uri=uri,
    #                            band_uris=band_uris,
    #                            app_info=task.virtual_product_def,
    #                            )
    odc_dataset = {}

    # write data to disk
    dataset_to_geotif_yaml(
        dataset=output_data,
        odc_dataset_metadata=odc_dataset,
        filename=base_filename,
        variable_params=variable_params,
    )

    return base_filename


def _import_transform(transform_name: str) -> Transformation:
    module_name, class_name = transform_name.rsplit('.', maxsplit=1)
    module = importlib.import_module(name=module_name)
    return getattr(module, class_name)()
